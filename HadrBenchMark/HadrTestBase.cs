﻿using MasterSlaveController;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Smo = Microsoft.SqlServer.Management.Smo;

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
        private Smo.Server secondary;

        private string primaryEndpointUrl;
        private string secondaryEndpointUrl;


        private string primaryServerName;
        private string secondaryServerName;
        private string agName;



        private int dbCount;

        public List<string> primaryDbsNames;
        public List<Smo.Database> primaryDbs;
        public List<Smo.Database> secondaryDBs;


        private MessageClient client;
        private List<string> notConnectedDBs;

        public void Setup()
        {
            agName = "HadrBenchTest";
            primaryServerName = @"ze-bench-01\hadrBenchMark01";
            secondaryServerName = @"ze-bench-02\hadrBenchMark01";

            baseDBpath = @"\\zechen-d1\dbshare\";
            dbshare = @"\\zechen-d1\dbshare\bench\";
            //primaryServerName = @"ze-2016-v1\sql16rtm01";
            //secondaryServerName = @"ze-2016-v2\sql16rtm01";
            // start point of dbCount, lets say 500
            primaryDbsNames = new List<string>();
            primaryDbs = new List<Database>();

            dbCount = 0;
            // get connection to primary and secondary server
            primary = new Smo.Server(primaryServerName);
            secondary = new Smo.Server(secondaryServerName);

            this.primaryEndpointUrl = ARHelper.GetHadrEndpointUrl(primary);
            this.secondaryEndpointUrl = ARHelper.GetHadrEndpointUrl(secondary);

            Console.WriteLine("Create hadrEnpoint Url: {0}", primaryEndpointUrl);
            Console.WriteLine("Create hadrEnpoint Url: {0}", secondaryEndpointUrl);
            TestNodesHADREnabled();
            if (!AGHelper.IsAGExist(agName, primary))
            {
                TestCreateAGWithTwoReplicasWithoutDatabase();
            }

            this.notConnectedDBs = new List<string>();

            //CreateBaselineDatabase();
            client = new MessageClient(11000);
            client.Setup();
            Console.WriteLine("complete connection");

        }

        public void CleanUp()
        {
            AGHelper.DropAG(agName, primary);
            CleanupDatabases();
        }

        public void TestNodesHADREnabled()
        {
            bool primaryEnabled = primary.IsHadrEnabled;
            if (!primaryEnabled)
            {
                Console.WriteLine("The server: {0} is {1} enabled HADR", primary.Name, primaryEnabled ? "" : "not");
            }

            bool secondaryEnabled = secondary.IsHadrEnabled;
            if (!secondaryEnabled)
            {
                Console.WriteLine("The server: {0} is {1} enabled HADR", secondary.Name, secondaryEnabled ? "" : "not");
            }

        }

        // When we called this primary and secondary should being create
        public void TestCreateAGWithTwoReplicasWithoutDatabase()
        {

            AvailabilityGroup ag = new AvailabilityGroup(primary, agName);

            AvailabilityReplica ar1 = ARHelper.BuildAR(ag, primary.Name, primaryEndpointUrl);
            AvailabilityReplica ar2 = ARHelper.BuildAR(ag, secondary.Name, secondaryEndpointUrl);



            ag.AvailabilityReplicas.Add(ar1);
            ag.AvailabilityReplicas.Add(ar2);

            try
            {
                Console.WriteLine("Creating availability group '{0}' on server '{1}",
                                            ag.Name, primary.Name);

                ag.Create();
                Thread.Sleep(1000); //Sleep a tick to let AG create take effect

                secondary.JoinAvailabilityGroup(agName);
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
                AGDBHelper.JoinAG(dbname, agName, secondary);
                Thread.Sleep(1000);
                // wait a bit to let adb join ag
            }
        }

        public void CreateBaselineDatabase()
        {
            dbCount = 10;
            for (int i = 1; i <= dbCount; i++)
            {
                primaryDbsNames.Add(string.Format("DB_{0}", i));
            }
            foreach (string dbName in primaryDbsNames)
                {
                    if (!primary.Databases.Contains(dbName))
                    {
                        Database db = new Database(primary, dbName);
                        db.Create();
                        primaryDbs.Add(db);

                        AGDBHelper.BackupDatabase(dbshare, primary, dbName);
                    }
                }
            AddDatabasesIntoAG(primaryDbsNames);
        }

        public void CreateDatabaseFromBackup(List<string> newDBNames)
        {
            foreach (string dbName in newDBNames)
            {
                if (!primary.Databases.Contains(dbName))
                {
                    AGDBHelper.RestoreDatabaseWithRename(baseDBpath, primary, "Test", dbName, false);
                    if (primary.Databases.Contains(dbName))
                    {
                        Database db = primary.Databases[dbName];
                        primaryDbs.Add(db);

                        db.RecoveryModel = RecoveryModel.Full;
                        db.Alter();

                        AGDBHelper.BackupDatabase(dbshare, primary, dbName);
                        AGDBHelper.RestoreDatabaseWithRename(dbshare, secondary, dbName, dbName, true, true);
                    }
                }
            }
        }

        public void ScanDBsFromEnvironment()
        {
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
                Console.WriteLine("Get range from {0} for {0}", index, magicNumber);
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

            Console.WriteLine("Get range from {0} for {0}", index, magicNumber);
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
