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
        private List<Smo.Server> replicas;
        private List<Smo.Server> secondaries;

        private List<string> replicaEndpointUrls;

        private string primaryServerName;
        private string agName;



        private int dbCount;

        public List<string> primaryDbsNames;
        public List<Smo.Database> primaryDbs;
        public List<Smo.Database> secondaryDBs;



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
            AGHelper.DropAG(agName, primary);
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
                Console.WriteLine("Running log backup for {0} databases at background", primaryDbs.Count);
                foreach (Database db in primaryDbs)
                {
                    AGDBHelper.LogBackup(dbshare, primary, db.Name);
                }
                Thread.Sleep(new TimeSpan(0, 2, 0));
            }

        }

        public void ScanDBsFromEnvironment()
        {
            // problem is here
            primary.Databases.Refresh();
            foreach (Database db in primary.Databases)
            {
                if (!db.IsSystemObject)

                {
                    primaryDbs.Add(db);
                    primaryDbsNames.Add(db.Name);
                    dbCount += 1;
                }
            }
            Console.WriteLine("Scan lab environment and found {0} databases", dbCount);

        }

        // add x databases into primary replica
        // keep database name in primaryDbNames in record

        public void AddDatabases(int num)
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



        }

        public void CleanupDatabases()
        {
            try
            {
                Console.WriteLine("Close all traffic.");
                // cleanup client
                client.Close();

                Console.WriteLine("Cleanning up databases");
                // instead of cleanup database only in the primary list, but we want to clean up every databases at nodes. 
                List<Database> primarydbList = new List<Database>();

                foreach (Database db in primary.Databases)
                {
                    if (!db.IsSystemObject)
                        primarydbList.Add(db);

                }
                if (primaryDbs != null)
                {
                    foreach (Database db in primarydbList)
                    {
                        if (db.State != SqlSmoState.Dropped)
                        {
                            // assume all databases should dropped from ag
                            db.Drop();
                        }
                    }
                    primaryDbs.Clear();
                }
                secondaryDBs = new List<Database>();
                foreach (Database db in secondary.Databases)
                {
                    if (!db.IsSystemObject)
                        secondaryDBs.Add(db);

                }
                if (secondaryDBs != null)
                {
                    foreach (Database db in secondaryDBs)
                    {
                        if (db.State != SqlSmoState.Dropped)
                        {
                            // assume all databases should dropped from ag
                            db.Drop();
                        }
                    }
                    secondaryDBs.Clear();
                }


            }catch(FailedOperationException e)
            {
                Console.WriteLine(e.InnerException);
                client.Close();
            }

        }
        // drain all databases from notConnected to traffic simulator
        public void BulkTraffic_v2(List<string> dblist, bool CleanupList)
        {
            int dbCount = dblist.Count;
            int slaveCount = client.GetServerCount();

            int count = 0;
            int index = 0;
            int magicNumber = 10;
            List<string> sublist;
            for (; (index +magicNumber) < dbCount; index += magicNumber)
            {
                Console.WriteLine("Get range from {0} to {0}", index, magicNumber);
                sublist = dblist.GetRange(index, magicNumber);
                //client.SendDbMessage(sublist);
                count += 1;
                if (count >3)
                {
                    count = 0;
                    Thread.Sleep(new TimeSpan(0, 2, 0));
                }
            }

            int leftover = dbCount - index;

            Console.WriteLine("Get range from {0} to {0}", index, magicNumber);
            sublist = dblist.GetRange(index, leftover);
            //client.SendDbMessage(sublist);

            if (CleanupList)
            {
                dblist.Clear();
            }
        }

        // drain all databases from notConnected to traffic simulator
        public void BulkTraffic(List<string> dblist, bool CleanupList)
        {
            int dbCount = dblist.Count;
            int slaveCount = client.GetServerCount();

            int divide = dbCount / slaveCount;
            int startIndex = 0;

            if (divide != 0)
            {
                for (int i = 0; i < slaveCount; i++)
                {

                    List<string> sublist = dblist.GetRange(startIndex, divide);
                    startIndex += divide;

                    client.SendDbMessage(sublist);
                }
            }

            // reminder db
            int reminder = dbCount % slaveCount;
            if (reminder != 0)
            {
                List<string> sublist = dblist.GetRange(startIndex, reminder);
                client.SendDbMessage(sublist);
            }
            if (CleanupList)
            {
                dblist.Clear();
            }
        }

        public void StartTraffic()
        {
            BulkTraffic_v2(notConnectedDBs, true);
        }

        public void RefreshTraffic()
        {
            client.Setup();
            BulkTraffic_v2(primaryDbsNames, false);
        }

        public void DrainTraffic()
        {
            Console.WriteLine("Close all traffic.");
            // cleanup client
            client.Close();
        }
    }
}
