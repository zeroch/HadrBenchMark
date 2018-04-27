using System;
using System.Collections.Generic;
using System.Text;

using Microsoft.SqlServer.Management.Smo;
using Smo = Microsoft.SqlServer.Management.Smo;

namespace HadrBenchMark
{

    public class HadrGUIConstants
    {
        public const int TimeoutForSsms = 60000;   // time out for waiting for SSMS to show up.
        public const int TimeoutForApplications = 60000;   // time out for waiting for CM to show up.
        public const int TimeoutForDialogs = 6000;   // time out for waiting for dialog to show up.
        public const int TimeoutForAction = 180000;  // time out for action (Creation, deletion, addition, join) to finish
        public const int TimeoutForQuery = 6000;  // time out for action (Creation, deletion, addition, join) to finish
        public const int TimeoutForOE = 120000;
        public const int RetryForOE = 5;

        public const short PrimaryServerId = 0;
        public const short SecondaryServer1Id = 1;
        public const short SecondaryServer2Id = 2;

        public const string DefaultInstance = "MSSQLSERVER";
        public const string WinAuthentication = "Windows Authentication";

        public const string hadrVersion = "10.50";

        public static Version hadrSupportSqlVersion = new Version(10, 50);
        public static Version hadrSupportOSVersion = new Version(6, 0);

        public const int HadrEndpointPort = 5022;
        public const string HadrEndpointName = "Hadr_endpoint";

        public static string[] ReadModeTexts = new string[] { "Disallow Connections", "Allow Only Read Intent Connections", "Allow All Connections" };

        public static string ConfigurationManagerCommand = "sqlservermanager11.msc";

        public static string oeAddDBToAGWizard = "Add Database...";
        public static string oeDatabaseRootPath = "Databases";
        public static string oeAvailabilityGroupProperties = "Properties";
        public static string oeAlwaysOnHighAvailabilityRootPath = "AlwaysOn High Availability";
        public static string oeAvailabilityGroupRootPath = "AlwaysOn High Availability\\Availability Groups";
        public static string oeAvailabilityReplicas = "Availability Replicas";
        public static string oeAvailabilityDatabases = "Availability Databases";
        public static string oeAvailabilityGroupListeners = "Availability Group Listeners";
        public static string[] oeAvailabilityGroupSubFolders = new string[] { oeAvailabilityReplicas, oeAvailabilityDatabases, oeAvailabilityGroupListeners };

        public static string oeNewAvailabilityGroupWizard = "New Availability Group Wizard...";
        public static string oeAddReplicaToAGWizard = "Add Replica...";
        public static string oeAddListener = "Add Listener...";
        public static string oeAGListenerProperties = "Properties";
        public static string oeNewAvailabilityGroupDialog = "New Availability Group...";
        public static string oeSuspendDataMenu = "Suspend Data Movement...";
        public static string oeResumeDataMenu = "Resume Data Movement...";
        public static string oeRemoveDatabaseFromPrimaryReplica = "Remove Database from Availability Group...";
        public static string oeRemoveDatabaseFromSecondaryReplica = "Remove Secondary Database...";
        public static string oeJoinAvailabilityGroup = "Join to Availability Group...";
        public static string oeFailoverMenu = "Failover...";
        public static string oeDeleteOperation = "Delete...";
        public static string oeRemoveSecondaryReplica = "Remove from Availability Group...";

        public static string oeDeleteKey = "{DEL}";

        public static string DatabaseStateRestoring = "Restoring...";
        public static string DatabaseStateNotSynchronizing = "Not Synchronizing";
        public static string DatabaseStateSynchronizing = "Synchronizing";
        public static string DatabaseStateSynchronized = "Synchronized";

        // SMO conversions to  UI values for VSTS #768823 workaround
        public static string AvReplSecConnModeAllowNoConnections = "no";
        public static string AvReplSecConnModeAllowReadIntentOnly = "allowread-intentonly";
        public static string AvReplSecConnModeAllowAllConnections = "yes";


        // AG Property Constants

        public const string AvailabilityModeAsynchronous = "Asynchronous commit";
        public const string AvailabilityModeSynchronous = "Synchronous commit";

        public const string FailoverModeManual = "Manual";
        public const string FailoverModeAutomatic = "Automatic";

        public const string ConnectionsInPrimaryRoleAllowAll = "Allow all connections";
        public const string ConnectionsInPrimaryRoleReadWrite = "Allow read/write connections";

        public const string ReadableSecondaryNo = "No";
        public const string ReadableSecondaryReadIntentOnly = "Read-intent only";
        public const string ReadableSecondaryYes = "Yes";

        public const bool ChangePrimary = true;
        public const bool ChangeSecondary = false;

        public const int FieldAvailabilityMode = 2;
        public const int FieldFailoverMode = 3;
        public const int FieldPrimaryConnectionRole = 4;
        public const int FieldSecondaryReadable = 5;



        public static List<string> SynchronizationStateEnums = new List<string>(new string[] { "Not Synchronizing", "Synchronizing", "Synchronized" });
    }
    public class AGHelper
    {
        /// <summary>
        /// Creates availability group using Smo
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="dbNames">Datbases to be part of the AG.</param>
        /// <param name="replicas">replicas to be part of the AG.</param>
        /// <param name="server">smo server.</param>
        public static AvailabilityGroup CreateAG(string agName, IEnumerable<string> dbNames, IEnumerable<Smo.Server> replicas, Smo.Server server)
        {
            if (!AGHelper.IsAGExist(agName, server))
            {
                AvailabilityGroup ag = new AvailabilityGroup(server, agName);
                foreach (string dbName in dbNames)
                {
                    ag.AvailabilityDatabases.Add(new AvailabilityDatabase(ag, dbName));
                }

                foreach (Smo.Server replica in replicas)
                {
                    AvailabilityReplica ar = new AvailabilityReplica(ag, replica.Name);
                    ar.EndpointUrl = ARHelper.GetHadrEndpointUrl(replica);
                    ar.AvailabilityMode = AvailabilityReplicaAvailabilityMode.AsynchronousCommit;
                    ar.FailoverMode = AvailabilityReplicaFailoverMode.Manual;
                    ag.AvailabilityReplicas.Add(ar);
                }
                ag.Create();
                return ag;
            }
            else
            {
                throw new Exception(string.Format("The requested availability Group {0} already exist in the given server {1}", agName, server.Name));
            }
        }

        /// <summary>
        /// Configure an Availability group. This performs the following steps
        /// <list type="number">
        /// <item>Join the secondary replica to the AG.</item>
        /// <item>Backup the databases from primary replica and restores it in secondary.</item>
        /// <item>Joins the database to primary.</item>
        /// </list>
        /// </summary>
        /// <param name="agName"></param>
        /// <param name="primaryReplica"></param>
        /// <param name="secondaryReplica"></param>
        /// <param name="databases"></param>
        /// <param name="fileShare"></param>
        public static void ConfigureAG(string agName, Smo.Server primaryReplica, Smo.Server secondaryReplica, IEnumerable<Database> databases, string fileShare)
        {
            ARHelper.JoinAG(agName, secondaryReplica);

            foreach (Database db in databases)
            {
                AGDBHelper.BackUpAndRestoreDatabase(fileShare, primaryReplica, secondaryReplica, db.Name);
            }

            foreach (Database db in databases)
            {
                AGDBHelper.JoinAG(db.Name, agName, secondaryReplica);
            }
        }

        /// <summary>
        /// Configure an Availability group for multiple secondary replicas. This performs the following steps
        /// <list type="number">
        /// <item>Backup databases on primary.</item>
        /// <item>Join each secondary replica to the AG.</item>
        /// <item>Restore databases on each secondary replica.</item>
        /// <item>Join databases to AG on each secondary replica.</item>
        /// </list>
        /// </summary>
        /// <param name="agName"></param>
        /// <param name="primaryReplica"></param>
        /// <param name="secondaries"></param>
        /// <param name="databases"></param>
        /// <param name="fileShare"></param>
        public static void ConfigureAGforMS(string agName, Smo.Server primaryReplica, IEnumerable<Smo.Server> secondaries, IEnumerable<Database> databases, string fileShare)
        {
            foreach (Database db in databases)
            {
                AGDBHelper.BackupDatabase(fileShare, primaryReplica, db.Name);
            }

            foreach (Smo.Server secondaryReplica in secondaries)
            {
                ARHelper.JoinAG(agName, secondaryReplica);

                foreach (Database db in databases)
                {
                    AGDBHelper.RestoreDatabase(fileShare, secondaryReplica, db.Name);
                }

                foreach (Database db in databases)
                {
                    AGDBHelper.JoinAG(db.Name, agName, secondaryReplica);
                }
            }
        }

        /// <summary>
        /// Enable an Availability Group.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="server">smo server.</param>
        public static AvailabilityGroup EnableAG(string agName, Smo.Server server)
        {
            if (!AGHelper.IsAGExist(agName, server))
            {
                AvailabilityGroup ag = server.AvailabilityGroups[agName];
                //TODO: AvailabilityGroup doesn't have method to enable it
                throw new NotImplementedException();
            }
            else
            {
                throw new Exception(string.Format("The requested availability Group {0} is not exist in the given server {1}", agName, server.Name));
            }
        }

        /// <summary>
        /// Disable an Availability Group.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="server">smo server.</param>
        public static AvailabilityGroup DisableAG(string agName, Smo.Server server)
        {
            if (!AGHelper.IsAGExist(agName, server))
            {
                AvailabilityGroup ag = server.AvailabilityGroups[agName];
                //TODO: AvailabilityGroup doesn't have method to disable it
                throw new NotImplementedException();
            }
            else
            {
                throw new Exception(string.Format("The requested availability Group {0} is not exist in the given server {1}", agName, server.Name));
            }
        }

        /// <summary>
        /// Checks whether the give availability groups is enabled or not.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="server">smo server.</param>
        /// <returns>true if the given ag is enabled, otherwise false.</returns>
        public static bool IsAGEnabled(string agName, Smo.Server server)
        {
            if (!AGHelper.IsAGExist(agName, server))
            {
                AvailabilityGroup ag = server.AvailabilityGroups[agName];
                //TODO: AvailabilityGroup doesn't have method to enable it
                throw new NotImplementedException();
            }
            else
            {
                throw new Exception(string.Format("The requested availability Group {0} is not exist in the given server {1}", agName, server.Name));
            }
        }

        /// <summary>
        /// Checks whether the give availability groups is already exist or not using Smo.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="server">smo server.</param>
        /// <returns>true if the given ag exist, otherwise false.</returns>
        public static bool IsAGExist(string agName, Smo.Server server)
        {
            server.AvailabilityGroups.Refresh();
            return server.AvailabilityGroups.Contains(agName);
        }

        /// <summary>
        /// Avaiability group is online if property PrimaryReplicaServerName returns valid value.
        /// </summary>
        /// <param name="agName">Availability Group Name</param>
        /// <param name="server">Target server name. This server must be the primary replica, else ths method will always return false.</param>
        /// <returns></returns>
        public static bool IsAgOnline(string agName, Smo.Server server)
        {
            return !string.IsNullOrEmpty(AGHelper.GetAG(agName, server).PrimaryReplicaServerName);
        }

        /// <summary>
        /// Drop the given ag using Smo.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="server">smo server.</param>
        public static void DropAG(string agName, Smo.Server server)
        {
            if (AGHelper.IsAGExist(agName, server))
            {
                AvailabilityGroup ag = server.AvailabilityGroups[agName];
                ag.Drop();
            }
        }

        /// <summary>
        /// Drop the given ag using Smo and wait until, the ag is dropped in all servers.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="targetServer">Server on which the AG needs to be dropped</param>
        /// <param name="otherServer">Check whether the AG is dropped on this server</param>
        public static void DropAGAndWaitForDrop(string agName, Smo.Server targetServer, Smo.Server otherServer)
        {
            AGHelper.DropAG(agName, targetServer);
        }

        /// <summary>
        /// Checks whether the given AG is dropped from all servers.
        /// </summary>
        /// <param name="input"></param>
        /// <returns>true, if ag is dropped from all servers; otherwise false.</returns>
        private static bool IsAGDropped(object[] input)
        {
            string agName = input[0] as string;
            IEnumerable<Smo.Server> servers = input[1] as IEnumerable<Smo.Server>;

            bool agDropped = true;
            foreach (Smo.Server server in servers)
            {
                server.AvailabilityGroups.Refresh();
                agDropped = agDropped && !server.AvailabilityGroups.Contains(agName);
            }
            return agDropped;
        }



        /// <summary>
        /// Checks whether the given list of replicas exist in the given AG.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="replicaNames">list of replicas to check against the AG</param>
        /// <param name="server">smo server.</param>
        /// <returns>true if all replicas are exist in AG, false otherwise.</returns>
        public static bool AGContainsReplicas(string agName, IEnumerable<string> replicaNames, Smo.Server server)
        {
            bool result = true;
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            foreach (string replicaName in replicaNames)
            {
                result = result && ag.AvailabilityReplicas.Contains(replicaName);
            }
            return result;
        }

        /// <summary>
        /// Adds a given replica to an Availability Group.
        /// </summary>
        /// <param name="agName"></param>
        /// <param name="replicaName"></param>
        /// <param name="server"></param>
        public static void AddReplicaToAG(string agName, string replicaName, Smo.Server server)
        {
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            AvailabilityReplica replica = new AvailabilityReplica(ag, replicaName);
            ag.AvailabilityReplicas.Add(replica);
            ag.Refresh();
            ag.AvailabilityReplicas.Refresh();
        }

        /// <summary>
        /// Removes a replica from an Availability Group.
        /// </summary>
        /// <param name="agName"></param>
        /// <param name="replicaName"></param>
        /// <param name="server"></param>
        public static void RemoveReplicaFromAG(string agName, string replicaName, Smo.Server server)
        {
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            AvailabilityReplica ar;
            if (ag.AvailabilityReplicas.Contains(replicaName))
            {
                ar = ag.AvailabilityReplicas[replicaName];
                ar.Drop();
            }
            ag.AvailabilityReplicas.Refresh();
        }

        /// <summary>
        /// Checks whether the given list of databases exist in the given AG.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="databaseNames">list of databases to check against the AG</param>
        /// <param name="server">smo server.</param>
        /// <returns>true if all databases are exist in AG, false otherwise.</returns>
        public static bool AGContainsDatabases(string agName, IEnumerable<string> databaseNames, Smo.Server server)
        {
            bool result = true;
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            foreach (string databaseName in databaseNames)
            {
                result = result && ag.AvailabilityDatabases.Contains(databaseName);
            }
            return result;
        }

        /// <summary>
        /// Checks whether the given list of databases are joined to the given availability group on the given server instance.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="databaseNames">List of database names.</param>
        /// <param name="server">Server instance.</param>
        /// <returns>true if all databases exist in the AG and are joined, false otherwise. </returns>  
        public static bool DatabasesAreJoined(string agName, IEnumerable<string> databaseNames, Smo.Server server)
        {
            if (server == null)
            {
                throw new ArgumentNullException("server");
            }

            AvailabilityGroup ag = server.AvailabilityGroups[agName];

            if (ag == null)
            {
                throw new ArgumentException("The availability group " + agName + " does not exist on the server " + server.Name);
            }

            foreach (string databaseName in databaseNames)
            {
                var database = ag.AvailabilityDatabases[databaseName];
                if (database == null || !database.IsJoined)
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Checks whether the given list of databases doesn't exist in the given AG.
        /// </summary>
        /// <param name="agName">Availability group name.</param>
        /// <param name="databaseNames">list of databases to check against the AG</param>
        /// <param name="server">smo server.</param>
        /// <returns>true if ALL databases are not exist in AG, false otherwise.</returns>
        public static bool AGNotContainsDatabases(string agName, IEnumerable<string> databaseNames, Smo.Server server)
        {
            bool result = true;
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            foreach (string databaseName in databaseNames)
            {
                result = result && !ag.AvailabilityDatabases.Contains(databaseName);
            }
            return result;
        }

        /// <summary>
        /// Adds a database to an Availability Group.
        /// </summary>
        /// <param name="agName">Availability Group name</param>
        /// <param name="databaseName">Database name to be added to AG</param>
        /// <param name="server">smo server</param>
        public static void AddDatabaseToAG(string agName, string databaseName, Smo.Server server)
        {
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            AvailabilityDatabase db = new AvailabilityDatabase(ag, databaseName);
            ag.AvailabilityDatabases.Add(db);
            ag.AvailabilityDatabases.Refresh();
        }

        /// <summary>
        /// On primary, removes database from availability group. On secondary, unjoins the secondary database from availability group.
        /// </summary>
        /// <param name="agName">Availability Group name</param>
        /// <param name="databaseName">Database name to be removed from AG</param>
        /// <param name="server">smo server</param>
        public static void RemoveDatabaseFromAG(string agName, string databaseName, Smo.Server server)
        {
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            AvailabilityDatabase adb;
            if (ag.AvailabilityDatabases.Contains(databaseName))
            {
                adb = ag.AvailabilityDatabases[databaseName];
                if (ag.AvailabilityReplicas[server.Name].Role == AvailabilityReplicaRole.Primary)
                {
                    adb.Drop();
                }
                else
                {
                    adb.LeaveAvailabilityGroup();
                }
            }
            ag.AvailabilityDatabases.Refresh();
        }

        /// <summary>
        /// Removes a database from an Availability Group.
        /// </summary>
        /// <param name="agName">Availability Group name</param>
        /// <param name="databaseNames">Database names to be removed from AG</param>
        /// <param name="server">smo server</param>
        public static void RemoveDatabasesFromAG(string agName, IEnumerable<string> databaseNames, Smo.Server server)
        {
            foreach (string databaseName in databaseNames)
            {
                AGHelper.RemoveDatabaseFromAG(agName, databaseName, server);
            }
        }

        public static AvailabilityGroup GetAG(string agName, Smo.Server server)
        {
            server.Refresh();
            server.AvailabilityGroups.Refresh();
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            ag.Refresh();
            return ag;
        }


        public enum ActionType
        {
            ForceFailover,
            ManualFailover,
        }

        /// <summary>
        /// Fails over an AvailabilityGroup to the server specified in manner specified
        /// </summary>
        /// <param name="serverToFailoverTo">The server to failover the AG to</param>
        /// <param name="agName">The Ag to failover</param>
        /// <param name="action">The type of failover</param>
        public static void FailoverToServer(Smo.Server serverToFailoverTo, string agName, ActionType action)
        {
            AvailabilityGroup agJoined = serverToFailoverTo.AvailabilityGroups[agName];

            if (agJoined == null)
            {
            }
            else
            {

                switch (action)
                {
                    case ActionType.ForceFailover:
                        agJoined.FailoverWithPotentialDataLoss();
                        break;

                    case ActionType.ManualFailover:
                        agJoined.Failover();
                        break;

                    default:
                        break;
                }
            }
        }
    }
}
