using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using Microsoft.SqlServer.Management.Smo;

using SMO = Microsoft.SqlServer.Management.Smo;

namespace HadrBenchMark
{



        public class AGDBHelper
        {
            private static string backupFileNameTemplate = "{0}_{1}.bak";

            #region Public Methods

            /// <summary>
            /// Sets database in required state.
            /// </summary>
            /// <param name="agName"></param>
            /// <param name="dbName"></param>
            /// <param name="suspend"></param>
            /// <param name="server"></param>
            private static void SetDbState(string agName, string dbName, bool suspend, SMO.Server server)
            {
                AvailabilityGroup ag = server.AvailabilityGroups[agName];
                AvailabilityDatabase db = ag.AvailabilityDatabases[dbName];
                db.Refresh(); // database state is server side property, so the SMO object needs to be refreshed to get the correct value.

                if (suspend)
                {
                    db.SuspendDataMovement();
                }
                else
                {
                    db.ResumeDataMovement();
                }


            }

            public static void SetDbOnline(string agName, string dbName, SMO.Server server)
            {
                AGDBHelper.SetDbState(agName, dbName, false, server);
            }

            public static void SetDbSuspended(string agName, string dbName, SMO.Server server)
            {
                AGDBHelper.SetDbState(agName, dbName, true, server);
            }
            private static bool IsDbSuspendedStateReached(Object[] input)
            {
                AvailabilityDatabase db = input[0] as AvailabilityDatabase;
                bool isSuspended = (bool)input[1];

                db.Refresh();
                return (db.IsSuspended == isSuspended);
            }

            public static bool GetDbSuspendedState(string agName, string dbName, SMO.Server server)
            {
                AvailabilityDatabase db = GetDb(agName, dbName, server);
                db.Refresh(); // database state is server side property, so the SMO object needs to be refreshed to get the correct value.
                return db.IsSuspended;
            }

            public static bool GetDbJoinedState(string agName, string dbName, SMO.Server server)
            {
                AvailabilityDatabase db = GetDb(agName, dbName, server);
                db.Refresh(); // database state is server side property, so the SMO object needs to be refreshed to get the correct value.
                return db.IsJoined;
            }

            public static AvailabilityDatabase GetDb(string agName, string dbName, SMO.Server server)
            {
                AvailabilityGroup ag = server.AvailabilityGroups[agName];
                AvailabilityDatabase db = ag.AvailabilityDatabases[dbName];
                return db;
            }

            public static void BackUpAndRestoreDatabase(string fileShare, SMO.Server sourceServer, SMO.Server targetServer, string dbName)
            {
                AGDBHelper.BackupDatabase(fileShare, sourceServer, dbName);
                AGDBHelper.RestoreDatabase(fileShare, targetServer, dbName);
            }


            /// <summary>
            /// Backup Database and Log on from source server to specified backup share.
            /// </summary>
            /// <param name="fileShare">Location of backup files</param>
            /// <param name="sourceServer">Source server</param>
            /// <param name="dbName">Name of the database to be backed-up</param>
            public static void BackupDatabase(string fileShare, SMO.Server sourceServer, string dbName)
            {
                string backupFilePath;
                foreach (BackupActionType backupType in new List<BackupActionType> { BackupActionType.Database, BackupActionType.Log })
                {
                    string fileName = string.Format(backupFileNameTemplate, dbName, backupType.ToString());
                    backupFilePath = Path.Combine(fileShare, fileName);

                    //delete the backup file
                    File.Delete(backupFilePath);
                    try
                    {
                        BackupDeviceItem backupDeviceItem = new BackupDeviceItem(backupFilePath, DeviceType.File);
                        //backup the database from the source server
                        Backup backup = new Backup();

                        backup.Action = backupType;
                        backup.Database = dbName;
                        backup.Devices.Add(backupDeviceItem);
                        backup.Incremental = false;
                        backup.Initialize = true;
                        backup.LogTruncation = BackupTruncateLogType.Truncate;

                        backup.SqlBackup(sourceServer);

                    }
                    catch (Exception ex)
                    {
                        //if an exception happens, delete the file
                        File.Delete(backupFilePath);

                        throw ex;
                    }
                }
            }


            /// <summary>
            /// Restore Database and Log on target server from backup files with NoRecovery = true. Keep the backup files in their original locations.
            /// </summary>
            /// <param name="fileShare">Location of backup files</param>
            /// <param name="targetServer">Destination server</param>
            /// <param name="dbName">Name of the database to be restored</param>
            public static void RestoreDatabase(string fileShare, SMO.Server targetServer, string dbName)
            {
                RestoreDatabase(fileShare, targetServer, dbName, true, false);
            }


        /// <summary>
        /// Restore Database target server from backup files to create database
        /// </summary>
        /// <param name="fileShare">Location of backup files</param>
        /// <param name="targetServer">Destination server</param>
        /// <param name="dbName">Name of the database to be restored</param>
        /// <param name="noRecovery">Gets or sets a Restore.NoRecovery property value that determines whether the tail of the log is backed up and whether the database is restored into the 'Restoring' state</param>

        public static void RestoreDatabaseToCreateDB(string fileShare, SMO.Server targetServer, string dbName, string newDbName, bool noRecovery = false)
        {
            string backupFilePath;
            string dataDirectory = targetServer.InstallDataDirectory;

            BackupActionType backupType = BackupActionType.Database;
            backupFilePath = Path.Combine(fileShare, string.Format(backupFileNameTemplate, dbName, backupType.ToString()));

            BackupDeviceItem backupDeviceItem = new BackupDeviceItem(backupFilePath, DeviceType.File);

            //restore on the destination
            Restore restore = new Restore();
            restore.Action = RestoreActionType.Database;
            restore.NoRecovery = noRecovery;

            restore.Devices.Add(backupDeviceItem);
            restore.Database = newDbName;
            restore.ReplaceDatabase = false;
            DataTable logicalFilesDt = restore.ReadFileList(targetServer);
            DataRow[] foundLogicalFilesRows = logicalFilesDt.Select();
            if (!string.IsNullOrEmpty(dataDirectory))
            {
                foreach (DataRow row in foundLogicalFilesRows)
                {
                    string logicalFileName = row["LogicalName"].ToString();
                    string physicalFileName = (logicalFileName.EndsWith("_log", StringComparison.OrdinalIgnoreCase)) ?
                        Path.Combine(dataDirectory, string.Format(CultureInfo.InvariantCulture, "{0}.ldf", newDbName)) :
                        Path.Combine(dataDirectory, string.Format(CultureInfo.InvariantCulture, "{0}.mdf", newDbName));
                    restore.RelocateFiles.Add(new RelocateFile(logicalFileName, physicalFileName));
                }
            }

            restore.SqlRestore(targetServer);
        }
        /// <summary>
        /// Restore Database and Log on target server from backup files.
        /// </summary>
        /// <param name="fileShare">Location of backup files</param>
        /// <param name="targetServer">Destination server</param>
        /// <param name="dbName">Name of the database to be restored</param>
        /// <param name="noRecovery">Gets or sets a Restore.NoRecovery property value that determines whether the tail of the log is backed up and whether the database is restored into the 'Restoring' state</param>

        public static void RestoreDatabaseWithRename(string fileShare, SMO.Server targetServer, string dbName, string newDbName, bool deleteBackupFiles, bool noRecovery = false)
        {
            string backupFilePath;
            string dataDirectory = targetServer.InstallDataDirectory;

            foreach (BackupActionType backupType in new List<BackupActionType> { BackupActionType.Database, BackupActionType.Log })
            {
                
            backupFilePath = Path.Combine(fileShare, string.Format(backupFileNameTemplate, dbName, backupType.ToString()));

            BackupDeviceItem backupDeviceItem = new BackupDeviceItem(backupFilePath, DeviceType.File);

            //restore on the destination
            Restore restore = new Restore();
            restore.Action =  RestoreActionType.Database;
            restore.NoRecovery = (backupType == BackupActionType.Log && noRecovery == false) ? false : true;

                restore.Devices.Add(backupDeviceItem);
            restore.Database = newDbName;
            restore.ReplaceDatabase = false;
            DataTable logicalFilesDt = restore.ReadFileList(targetServer);
            DataRow[] foundLogicalFilesRows = logicalFilesDt.Select();
            if (!string.IsNullOrEmpty(dataDirectory))
            {
                foreach (DataRow row in foundLogicalFilesRows)
                {
                    string logicalFileName = row["LogicalName"].ToString();
                    string physicalFileName = (logicalFileName.EndsWith("_log", StringComparison.OrdinalIgnoreCase)) ?
                        Path.Combine(dataDirectory, string.Format(CultureInfo.InvariantCulture, "{0}.ldf", newDbName)) :
                        Path.Combine(dataDirectory, string.Format(CultureInfo.InvariantCulture, "{0}.mdf", newDbName));
                    restore.RelocateFiles.Add(new RelocateFile(logicalFileName, physicalFileName));
                }
            }

                restore.SqlRestore(targetServer);
                if (deleteBackupFiles)
                {
                    File.Delete(backupFilePath);
                }
            }
        }

            /// <summary>
            /// Restore Database and Log on target server from backup files.
            /// </summary>
            /// <param name="fileShare">Location of backup files</param>
            /// <param name="targetServer">Destination server</param>
            /// <param name="dbName">Name of the database to be restored</param>
            /// <param name="noRecovery">Gets or sets a Restore.NoRecovery property value that determines whether the tail of the log is backed up and whether the database is restored into the 'Restoring' state</param>
            /// <param name="deleteBackupFiles">If true, backup files will be deleted after restore operation is complete</param>
            public static void RestoreDatabase(string fileShare, SMO.Server targetServer, string dbName, bool noRecovery, bool deleteBackupFiles)
            {
                string backupFilePath;
                string dataDirectory = targetServer.InstallDataDirectory;

                foreach (BackupActionType backupType in new List<BackupActionType> { BackupActionType.Database, BackupActionType.Log })
                {
                    backupFilePath = Path.Combine(fileShare, string.Format(backupFileNameTemplate, dbName, backupType.ToString()));

                    BackupDeviceItem backupDeviceItem = new BackupDeviceItem(backupFilePath, DeviceType.File);

                    //restore on the destination
                    Restore restore = new Restore();
                    restore.Action = (backupType == BackupActionType.Database) ? RestoreActionType.Database : RestoreActionType.Log;
                    restore.NoRecovery = (backupType == BackupActionType.Log && noRecovery == false) ? false : true;
                    restore.Devices.Add(backupDeviceItem);
                    restore.Database = dbName;
                    restore.ReplaceDatabase = false;
                    DataTable logicalFilesDt = restore.ReadFileList(targetServer);
                    DataRow[] foundLogicalFilesRows = logicalFilesDt.Select();
                    if (!string.IsNullOrEmpty(dataDirectory))
                    {
                        foreach (DataRow row in foundLogicalFilesRows)
                        {
                            string logicalFileName = row["LogicalName"].ToString();
                            string physicalFileName = (logicalFileName.EndsWith("_log", StringComparison.OrdinalIgnoreCase)) ?
                                Path.Combine(dataDirectory, string.Format(CultureInfo.InvariantCulture, "{0}.ldf", logicalFileName)) :
                                Path.Combine(dataDirectory, string.Format(CultureInfo.InvariantCulture, "{0}.mdf", logicalFileName));
                            restore.RelocateFiles.Add(new RelocateFile(logicalFileName, physicalFileName));
                        }
                    }

                    foreach (string script in restore.Script(targetServer))
                    {
                    }
                    restore.SqlRestore(targetServer);

                    if (deleteBackupFiles)
                    {
                        File.Delete(backupFilePath);
                    }
                }
            }






            /// <summary>
            /// Join the databases to an Availability Group.
            /// </summary>
            /// <param name="dbNames"></param>
            /// <param name="agName"></param>
            /// <param name="server"></param>
            public static void JoinAG(IEnumerable<string> dbNames, string agName, SMO.Server server)
            {
                foreach (string dbName in dbNames)
                {
                    JoinAG(dbName, agName, server);
                }
            }

            /// <summary>
            /// Join a database to an Availability Group.
            /// </summary>
            /// <param name="dbName"></param>
            /// <param name="agName"></param>
            /// <param name="server"></param>
            public static void JoinAG(string dbName, string agName, SMO.Server server)
            {
                AvailabilityDatabase aDb = GetDb(agName, dbName, server);
                aDb.JoinAvailablityGroup();
            }


            /// <summary>
            /// Verify availability database states match expected values.
            /// </summary>
            public static void VerifyDatabaseStates(SMO.Server server, string agName, string dbName, bool expJoinedState, bool expSuspendedState, AvailabilityDatabaseSynchronizationState expSyncState)
            {
                bool currState;

                currState = AGDBHelper.GetDbJoinedState(agName, dbName, server);

                currState = AGDBHelper.GetDbSuspendedState(agName, dbName, server);

                AvailabilityDatabase db = AGDBHelper.GetDb(agName, dbName, server);
                db.Refresh();
                AvailabilityDatabaseSynchronizationState currSyncState = db.SynchronizationState;
            }


            /// <summary>
            /// Wait for database synchronization state to reach target state.
            /// </summary>
            public static void WaitForDatabaseSynchronizationStateToGetUpdated(SMO.Server host, string agName, string dbName, AvailabilityDatabaseSynchronizationState expectedState, int timeout)
            {
                for (int i = 0; i < timeout; i += 1000)
                {
                    host.AvailabilityGroups[agName].AvailabilityDatabases[dbName].Refresh();
                    if (host.AvailabilityGroups[agName].AvailabilityDatabases[dbName].SynchronizationState == expectedState)
                    {
                        return;
                    }
                    System.Threading.Thread.Sleep(1000);
                }
            }
            #endregion

        }
    }


