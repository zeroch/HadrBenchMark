using MasterSlaveController;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using Smo = Microsoft.SqlServer.Management.Smo;
using System.Data.SqlClient;


namespace HadrBenchMark
{
    public class HadrTestBase
    {
        const int randomSeed = 9999;

        private string dbshare;
        private string baseDBpath;
        // connect to Primary
        private Smo.Server primary;
        // use only one secondary right now, refactor to list if need more replica
        private Smo.Server reportSrv;
        private List<Smo.Server> replicas;
        private List<Smo.Server> secondaries;
        
        private List<string> replicaEndpointUrls;

        private string primaryServerName;
        private string reportServerName;
        private string reportDBName;
        private string reportConnecionString;

        private string agName;



        private int dbCount;

        public List<string> primaryDbsNames;
        public List<Smo.Database> primaryDbs;
        public List<Smo.Database> secondaryDBs;

        ReaderWriterLockSlim dbCacheLock;

        private MessageClient client;
        private List<string> notConnectedDBs;

        private bool stopBackup;

        public void StopBackup()
        {
            stopBackup = true;
        }
        public void Setup()
        {
            agName = "HadrBenchTest";


            baseDBpath = @"\\zechen-d1\dbshare\";
            dbshare = @"\\zechen-d1\dbshare\bench\";
            //primaryServerName = @"ze-2016-v1\sql16rtm01";
            //secondaryServerName = @"ze-2016-v2\sql16rtm01";
            // start point of dbCount, lets say 500
            primaryDbsNames = new List<string>();
            primaryDbs = new List<Database>();

            dbCacheLock = new ReaderWriterLockSlim();

            dbCount = 0;
            // create three replicas

            replicas = new List<Server>();
            secondaries = new List<Server>();
            replicaEndpointUrls = new List<string>();
            primaryServerName = string.Empty;

            Smo.Server srv;
            srv = new Smo.Server(@"ze-bench-01\hadrBenchMark01");
            replicas.Add(srv);
            // primary is important
            primaryServerName = @"ze-bench-01\hadrBenchMark01";
            primary = srv;
            //report server always point to 01
            reportSrv = srv;
            reportServerName = primaryServerName;
            reportDBName = @"FailoverResult";
            string username = "reportA";
            string password = "report@123";
            reportConnecionString = string.Format("server={0}; Initial Catalog={1};uid={2}; pwd={3} ", primaryServerName, reportDBName, username, password);


            srv = new Smo.Server(@"ze-bench-02\hadrBenchMark01");
            replicas.Add(srv);
            secondaries.Add(srv);
            srv = new Smo.Server(@"ze-bench-03\hadrBenchMark01");
            replicas.Add(srv);
            secondaries.Add(srv);

            string replicaEndpointUrl = string.Empty;
            foreach(Smo.Server server in replicas)
            {
                replicaEndpointUrl = ARHelper.GetHadrEndpointUrl(server);
                replicaEndpointUrls.Add(replicaEndpointUrl);
            }



            if (!AGHelper.IsAGExist(agName, primary))
            {
                TestCreateAGWithTwoReplicasWithoutDatabase();
            }

            this.notConnectedDBs = new List<string>();


            stopBackup = false;


        }

        public void CleanUp()
        {
            if (AGHelper.IsAGExist(agName, primary))
            {
                AGHelper.DropAG(agName, primary);
            }
            CleanupDatabases();
            StopBackup();
        }



        // When we called this primary and secondary should being create
        public void TestCreateAGWithTwoReplicasWithoutDatabase()
        {
            Smo.Server primary = replicas[0];
            AvailabilityGroup ag = new AvailabilityGroup(primary, agName);

            List<Smo.Server> secondaries = replicas.GetRange(1, replicas.Count - 1);

            for(int i = 0; i < replicas.Count; ++i)
            {

                AvailabilityReplica ar = ARHelper.BuildAR(ag, replicas[i].Name, replicaEndpointUrls[i]);
                ag.AvailabilityReplicas.Add(ar);
            }


            try
            {
                Console.WriteLine("Creating availability group '{0}' on server '{1}",
                                            ag.Name, primary.Name);

                ag.Create();
                Thread.Sleep(1000); //Sleep a tick to let AG create take effect

                foreach(Smo.Server srv in secondaries)
                {
                    srv.JoinAvailabilityGroup(agName);
                    
                }
                CreateAGListener();
                // enable autoseeding in secondary
                //secondary.GrantAvailabilityGroupCreateDatabasePrivilege(agName);

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Failed to create AG {0}", ag.Name);
                AGHelper.DropAG(ag.Name, primary);
            }
        }

        public void AddDatabasesIntoAG(List<string> listDbNames)
        {
            AvailabilityGroup ag = primary.AvailabilityGroups[agName];
            if (ag == null)
            {
                return;
            }
            foreach(string dbname in listDbNames)
            {
                AvailabilityDatabase adb = new AvailabilityDatabase(ag, dbname);
                adb.Create();
                foreach(Smo.Server srv in secondaries)
                {
                    AGDBHelper.JoinAG(dbname, agName, srv);
                    Thread.Sleep(1000);
                }

                // wait a bit to let adb join ag
            }
        }


        public void CreateDatabaseFromBackup(List<string> newDBNames)
        {
            foreach (string dbName in newDBNames)
            {
                primary.Databases.Refresh();
                if (!primary.Databases.Contains(dbName))
                {
                    AGDBHelper.RestoreDatabaseWithRename(baseDBpath, primary, "Test", dbName, false);
                    primary.Databases.Refresh();
                    if (primary.Databases.Contains(dbName))
                    {
                        Database db = primary.Databases[dbName];
                        primaryDbs.Add(db);

                        db.RecoveryModel = RecoveryModel.Full;
                        db.Alter();

                        AGDBHelper.BackupDatabase(dbshare, primary, dbName);
                        foreach (Smo.Server srv in secondaries)
                        {
                            AGDBHelper.RestoreDatabaseWithRename(dbshare, srv, dbName, dbName, false, true);
                        }
                    }
                }
            }
        }

        public void BackupLog()
        {

            while(!stopBackup)
            {
                dbCacheLock.EnterReadLock();
                try
                {
                    Console.WriteLine("Running log backup for {0} databases at background", primaryDbs.Count);
                    foreach (Database db in primaryDbs)
                    {
                        AGDBHelper.LogBackup(dbshare, primary, db.Name);
                    }
                }finally
                {
                    dbCacheLock.ExitReadLock();
                }

                Thread.Sleep(new TimeSpan(0, 2, 0));
            }

        }

        public void ScanDBsFromEnvironment()
        {
            dbCacheLock.EnterWriteLock();
            try
            {
                // problem is here
                primary.Databases.Refresh();
                foreach (Database db in primary.Databases)
                {
                    if (!db.IsSystemObject)

                    {
                        if (db.Name != "FailoverResult")
                        {
                            primaryDbs.Add(db);
                            primaryDbsNames.Add(db.Name);
                            dbCount += 1;
                        }

                    }
                }
                Console.WriteLine("Scan lab environment and found {0} databases", dbCount);
            }finally
            {
                dbCacheLock.ExitWriteLock();
            }


        }

        // add x databases into primary replica
        // keep database name in primaryDbNames in record

        public void AddDatabases(int num)
        {
            dbCacheLock.EnterWriteLock();
            try
            {
                int index = dbCount + 1;
                dbCount += num;
                List<string> additionDBNames = new List<string>();
                for (; index <= dbCount; index++)
                {
                    string dbName = string.Format("DB_{0}", index);
                    additionDBNames.Add(dbName);
                    notConnectedDBs.Add(dbName);
                }

                CreateDatabaseFromBackup(additionDBNames);
                AddDatabasesIntoAG(additionDBNames);
                Console.WriteLine("Add {0} Database into AG", num);
            }finally
            {
                dbCacheLock.ExitWriteLock();
            }

        }

        public void CleanupDatabases()
        {
            try
            {
                //Console.WriteLine("Close all traffic.");
                // cleanup client
                //client.Close();

                Console.WriteLine("Cleanning up databases");
                // srv.Databases is a collection that doesn't allow to drop in run-time
                // so, we make a copy of clean list
                List<Database> cleanList = new List<Database>();
                foreach(Server srv in replicas)
                {
                    foreach(Database db in srv.Databases)
                    {
                        if ( db.Name.Contains("DB_"))
                        {
                            cleanList.Add(db);
                        }
                    }
                }
                
                foreach( Database db in cleanList)
                {
                    if (db.State != SqlSmoState.Dropped)
                    {
                        // assume all databases should dropped from ag
                        db.Drop();
                    }
                }


            }catch(FailedOperationException e)
            {
                Console.WriteLine(e.InnerException);

            }

        }
        // drain all databases from notConnected to traffic simulator
        public void BulkTraffic_v2(List<string> dblist, bool CleanupList, bool fullTraffic = true)
        {
            int dbCount = 0;
            if (fullTraffic)
            {
                dbCount = dblist.Count;
            }else
            {
                dbCount = dblist.Count / 10;
            }
            
            int slaveCount = client.GetServerCount();

            int count = 0;
            int index = 0;
            int magicNumber = 10;
            List<string> sublist;
            for (; (index +magicNumber) < dbCount; index += magicNumber)
            {
                Console.WriteLine("Get range from {0} to {0}", index, magicNumber);
                sublist = dblist.GetRange(index, magicNumber);
                client.SendDbMessage(sublist);
                count += 1;
                if (count >3)
                {
                    count = 0;
                    Thread.Sleep(new TimeSpan(0, 0, 30));
                }
            }

            int leftover = dbCount - index;

            Console.WriteLine("Get range from {0} to {0}", index, magicNumber);
            sublist = dblist.GetRange(index, leftover);
            client.SendDbMessage(sublist);

            if (CleanupList)
            {
                dblist.Clear();
            }
        }

        public void StartPartialTraffic()
        {
            //CreateBaselineDatabase();
            client = new MessageClient(11000);
            client.Setup();
            Console.WriteLine("complete connection");

            BulkTraffic_v2(primaryDbsNames, false, false);


        }

        public void StartFullTraffic()
        {
            //CreateBaselineDatabase();
            client = new MessageClient(11000);
            client.Setup();
            Console.WriteLine("complete connection");

            BulkTraffic_v2(primaryDbsNames, false, true);

        }

        public void DrainTraffic()
        {
            Console.WriteLine("Close all traffic.");
            // cleanup client
            client.Close();
        }

        // helper that runs query
        public void ExecuteQuery()
        {
            // I want to test 19 failovers here 
            int failoverCount = replicas.FindIndex(t => t.Name == primary.Name);
            // find current primary's index from replica list
            while(failoverCount < 21)
            {
                // pick new primary
                int primaryIndex = (failoverCount+1) % replicas.Count;
                Smo.Server newPrimary = replicas[primaryIndex];

                //Console.WriteLine("Checking AG Synced");
                while (!IsAGSynchronized())
                {
                    Thread.Sleep(10);
                }
                DateTime beforeFailover = DateTime.Now;

                AGHelper.FailoverToServer(newPrimary, agName, AGHelper.ActionType.ManualFailover);
                primary = newPrimary;
                Console.WriteLine("AG: {0} failover to {1}. ", agName, primary.Name);
                Console.WriteLine("Current AG is {0} synchronized", IsAGSynchronized() ? " " : "not");
                while (!IsAGSynchronized())
                {
                    Thread.Sleep(10);
                }
                DateTime afterFailover = DateTime.Now;
                TimeSpan failoverInterval = afterFailover - beforeFailover;
                Console.WriteLine("Failover takes {0}", failoverInterval.TotalSeconds);
                InsertFailoverReport(beforeFailover, afterFailover, primary.Databases.Count);
                failoverCount += 1;
                Thread.Sleep(new TimeSpan(0, 2, 0));


            }
            Console.WriteLine("Thread Stop");
        }

        public void InsertFailoverReport(DateTime before, DateTime after, int dbCount)
        {
            using (SqlConnection con = new SqlConnection(reportConnecionString))
            {
                con.Open();
                using (SqlCommand query = new SqlCommand("insert into failover_result(DBCount,failoverStartTime,failoverEndTime) VALUES (@dbcount, @before, @after)", con))
                {
                    query.Parameters.Add("@dbcount", SqlDbType.Int);
                    query.Parameters["@dbcount"].Value = dbCount;
                    query.Parameters.AddWithValue("@before", before);
                    query.Parameters.AddWithValue("@after", after);
                    query.ExecuteNonQuery();
                }
            }

        }


        public bool IsAGSynchronized()
        {
            string query = @"select COUNT(*) as 'UnHealthy Db'
FROM sys.dm_hadr_database_replica_states drs, sys.availability_groups ag, sys.databases dbs
WHERE drs.group_id = ag.group_id AND dbs.database_id = drs.database_id AND synchronization_health_desc <> 'HEALTHY'";

            DataSet ds = primary.ConnectionContext.ExecuteWithResults(query);
            DataRow dr = ds.Tables[0].Rows[0];

            //Console.WriteLine("UnHealthy Db: {0}", dr["UnHealthy Db"].ToString());
            return (int)dr["UnHealthy Db"] == 0;

        }

        public void CreateAGListener()
        {
            string query = @"   USE [master]
                                GO
                                ALTER AVAILABILITY GROUP [HadrBenchTest]
                                ADD LISTENER N'BenchTestLis' (
                                WITH DHCP
                                 ON (N'10.193.16.0', N'255.255.252.0'
                                )
                                , PORT=62444);
                                GO
                                ";

            primary.ConnectionContext.ExecuteNonQuery(query);

        }



    }
}
