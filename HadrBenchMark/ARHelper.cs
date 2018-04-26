using System;
using System.Collections.Generic;
using System.Text;

using Microsoft.SqlServer.Management.Smo;
using SMO = Microsoft.SqlServer.Management.Smo;
namespace HadrBenchMark
{
    public class ARHelper
    {
        public const int hadrEndpointPort = 5022;
        public const string hadrEndpointName = "Hadr_endpoint";

        public enum ReplicaAvailabilityMode
        {
            AsynchronousCommit,
            SynchronousCommit
        }

        public enum ReplicaFailoverMode
        {
            AutomaticFailover,
            ManualFailover
        }

        public static AvailabilityReplica BuildAR(AvailabilityGroup ag, string replicaName, string endpointUrl)
        {
            //This will give a different combination of the possible configurations of an AR every time it runs.
            AvailabilityReplica ar = new AvailabilityReplica(ag, replicaName);
            ar.AvailabilityMode = AvailabilityReplicaAvailabilityMode.SynchronousCommit;
            ar.FailoverMode = AvailabilityReplicaFailoverMode.Automatic;
            ar.EndpointUrl = endpointUrl;
            ar.SessionTimeout = 30;
            ar.ConnectionModeInPrimaryRole = AvailabilityReplicaConnectionModeInPrimaryRole.AllowAllConnections;
            ar.ConnectionModeInSecondaryRole = AvailabilityReplicaConnectionModeInSecondaryRole.AllowAllConnections;
            ar.BackupPriority = 3;
            ar.SeedingMode = AvailabilityReplicaSeedingMode.Automatic;
            return ar;

        }

        /// <summary>
        /// Drop availability replica from an availability group.
        /// NoOp, if availability group isn't exist or replica isn't part of the given AG.
        /// </summary>
        /// <param name="arName">Availability replica name</param>
        /// <param name="agName">Availability groups name</param>
        /// <param name="server">SMO server</param>
        public static void DropAR(string arName, string agName, SMO.Server server)
        {
            if (AGHelper.IsAGExist(agName, server) && AGHelper.AGContainsReplicas(agName, new string[] { arName }, server))
            {
                AvailabilityGroup ag = server.AvailabilityGroups[agName];
                ag.AvailabilityReplicas.Remove(arName);
            }
        }

        /// <summary>
        /// Updates the endpoint url of an AR.
        /// </summary>
        /// <param name="arName">Availability replica name.</param>
        /// <param name="agName">Availability grorup name.</param>
        /// <param name="EndpointUrl">endpoint url to be set.</param>
        /// <param name="server">smo server where this has to be executed.</param>
        public static void UpdateAREndpointUrl(string arName, string agName, string EndpointUrl, SMO.Server server)
        {
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            AvailabilityReplica ar = ag.AvailabilityReplicas[arName];
            ar.EndpointUrl = EndpointUrl;
        }

        /// <summary>
        /// Gets the endpoint url of an AR.
        /// </summary>
        /// <param name="arName">Availability replica name</param>
        /// <param name="agName">Availability grorup name</param>
        /// <param name="server">smo server where this has to be executed</param>
        /// <returns>Endpoint url of an Availability Replica</returns>
        public static string GetAREndpointUrl(string arName, string agName, SMO.Server server)
        {
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            AvailabilityReplica ar = ag.AvailabilityReplicas[arName];
            return ar.EndpointUrl;
        }

        /// <summary>
        /// Checks whether the given replica is primary replica on an Availability group or not.
        /// </summary>
        /// <param name="arName">Replica name that needs to be checked</param>
        /// <param name="agName">Availability Group name</param>
        /// <param name="server">SMO Server on which being checked.</param>
        /// <returns>True if the given replica is primary, otherwise false.</returns>
        public static bool IsPrimaryReplica(string arName, string agName, SMO.Server server)
        {
            server.Refresh();
            server.AvailabilityGroups.Refresh();
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            ag.Refresh();
            return (ag.PrimaryReplicaServerName.Equals(arName));
        }

        public static SMO.AvailabilityReplica GetPrimaryReplica(string agName, SMO.Server server)
        {
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            ag.Refresh();

            foreach (SMO.AvailabilityReplica replica in ag.AvailabilityReplicas)
            {
                if (replica.Role == AvailabilityReplicaRole.Primary)
                {
                    return replica;
                }
            }

            return null;
        }

        public static List<SMO.AvailabilityReplica> GetSecondaryReplicas(string agName, SMO.Server server)
        {
            List<SMO.AvailabilityReplica> secondaryReplicaList = new List<AvailabilityReplica>();
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            ag.Refresh();

            foreach (SMO.AvailabilityReplica replica in ag.AvailabilityReplicas)
            {
                if (replica.Role != AvailabilityReplicaRole.Primary)
                {
                    secondaryReplicaList.Add(replica);
                }
            }

            return secondaryReplicaList;
        }

        public static void ModifyFailoverModeOnPrimaryReplica(SMO.AvailabilityReplicaFailoverMode failoverMode, string availabilityGroupName, SMO.Server primaryReplicaServer)
        {
            SMO.AvailabilityReplica primaryReplica = ARHelper.GetPrimaryReplica(availabilityGroupName, primaryReplicaServer);

            primaryReplica.FailoverMode = failoverMode;
            primaryReplica.Alter();
        }

        public static void ModifyFailoverModeOnSecondaryReplicas(SMO.AvailabilityReplicaFailoverMode failoverMode, string availabilityGroupName, SMO.Server primaryReplicaServer)
        {
            foreach (SMO.AvailabilityReplica secondaryReplica in ARHelper.GetSecondaryReplicas(availabilityGroupName, primaryReplicaServer))
            {
                secondaryReplica.FailoverMode = failoverMode;
                secondaryReplica.Alter();
            }
        }


        /// <summary>
        /// Checks whether the given replica is synchronized or not.
        /// </summary>
        /// <param name="arName">Replica that needs to be checked</param>
        /// <param name="agName">Availability Group name</param>
        /// <param name="server">SMO Server on which check.</param>
        /// <returns>True if the given replica become synchronized, otherwise false.</returns>
        public static bool IsReplicaSynchronized(string arName, string agName, SMO.Server server)
        {
            AvailabilityReplica ar = server.AvailabilityGroups[agName].AvailabilityReplicas[arName];
            ar.Refresh();
            return (ar.RollupSynchronizationState.Equals(AvailabilityReplicaRollupSynchronizationState.Synchronized));
        }



        /// <summary>
        /// Gets endpoint url for a given server. If endpoint doesn't exist, it will be created.
        /// </summary>
        /// <param name="server">SMO server for which endpoint url needs to be retrieved.</param>
        /// <returns>Endpoinurl in string format.</returns>
        public static string GetHadrEndpointUrl(SMO.Server server)
        {
            Endpoint ep = ARHelper.CreateHadrEndpoint(server);

            return string.Format(@"TCP://{0}:{1}", System.Net.Dns.GetHostEntry(server.ComputerNamePhysicalNetBIOS).HostName.ToString(),
                ep.Protocol.Tcp.ListenerPort.ToString());
        }

        /// <summary>
        /// Creates endpoint on port 5022 on a given server. Skips creation if endpoint already exists.
        /// </summary>
        /// <param name="server">SMO server where endpoint needs to be created.</param>
        /// <returns>Newly created or existing endpoint.</returns>
        public static Endpoint CreateHadrEndpoint(SMO.Server server)
        {
            Endpoint ep = ARHelper.GetHadrEndpoint(server);
            if (null == ep)
            {
                ep = new Endpoint(server, hadrEndpointName);
                ep.EndpointType = EndpointType.DatabaseMirroring;
                ep.ProtocolType = ProtocolType.Tcp;
                ep.Payload.DatabaseMirroring.ServerMirroringRole = ServerMirroringRole.All;
                ep.Payload.DatabaseMirroring.EndpointEncryption = EndpointEncryption.Required;
                ep.Payload.DatabaseMirroring.EndpointEncryptionAlgorithm = EndpointEncryptionAlgorithm.Aes;
                ep.Create();
                ep.Start();
                if (ep.EndpointState != EndpointState.Started)
                {
                    throw new Exception(string.Format("Endpoint {0} on server {1} failed to start", ep.Name, server.Name));
                }
            }
            return ep;
        }

        /// <summary>
        /// Deletes endpoint on port 5022 from a given server. Noop if endpoint doesn't exist.
        /// </summary>
        /// <param name="server">SMO server from which endpoint needs to be deleted.</param>
        public static void DeleteHadrEndpoint(SMO.Server server)
        {
            Endpoint ep = ARHelper.GetHadrEndpoint(server);
            if (null != ep)
            {
                ep.Drop();
            }
        }

        /// <summary>
        /// Retreives HADR endpoint(on port 5022) from a given server.
        /// </summary>
        /// <param name="server">Target server.</param>
        /// <returns>Endpoint, NULL if endpoint doesn't exist on server.</returns>
        public static Endpoint GetHadrEndpoint(SMO.Server server)
        {
            // Need to call Refresh because server.Endpoints returns endpoint that has been just deleted.
            server.Endpoints.Refresh();

            foreach (Endpoint ep in server.Endpoints)
            {
                if (ep.EndpointType == EndpointType.DatabaseMirroring &&
                    ep.ProtocolType == ProtocolType.Tcp &&
                    ep.EndpointState == EndpointState.Started)
                {
                    return ep;
                }
            }
            return null;
        }

        /// <summary>
        /// Join the given availability replica to an Availability Group.
        /// Note that this method will not validate whether the given Server is
        /// already part of AG or it is already joined the AG, etc.
        /// </summary>
        /// <param name="agName">Name of the Availability Group</param>
        /// <param name="server">Secondary replica</param>
        public static void JoinAG(string agName, SMO.Server server)
        {
            server.JoinAvailabilityGroup(agName);

            // TODO: VSTS - 445952 - After joining a replica to an AG, the server.AvailabilityGroups collection doesn't contain the new replica.

        }


        public static AvailabilityReplica GetReplica(string agName, string replicaName, SMO.Server server)
        {
            AvailabilityGroup ag = server.AvailabilityGroups[agName];
            AvailabilityReplica ar = ag.AvailabilityReplicas[replicaName];
            return ar;
        }




        /// <summary>
        /// Wait for replica role to reach target state.
        /// </summary>
        public static void WaitForReplicaRoleToGetUpdated(SMO.Server host, string agName, string replicaName, AvailabilityReplicaRole expectedRole, int timeout)
        {
            for (int i = 0; i < timeout; i += 1000)
            {
                host.AvailabilityGroups[agName].AvailabilityReplicas[replicaName].Refresh();
                if (host.AvailabilityGroups[agName].AvailabilityReplicas[replicaName].Role == expectedRole)
                {
                    return;
                }
                System.Threading.Thread.Sleep(1000);
            }
        }


        /// <summary>
        /// Wait for replica rollup synchronization state to reach target state.
        /// </summary>
        public static void WaitForReplicaRollupSynchronizationStateToGetUpdated(SMO.Server host, string agName, string replicaName, AvailabilityReplicaRollupSynchronizationState expectedState, int timeout)
        {
            for (int i = 0; i < timeout; i += 1000)
            {
                host.AvailabilityGroups[agName].AvailabilityReplicas[replicaName].Refresh();
                if (host.AvailabilityGroups[agName].AvailabilityReplicas[replicaName].RollupSynchronizationState == expectedState)
                {
                    return;
                }
                System.Threading.Thread.Sleep(1000);
            }
        }


        /// <summary>
        /// Wait for replica connection state to reach target state.
        /// </summary>
        public static void WaitForReplicaConnectionStateToGetUpdated(SMO.Server host, string agName, string replicaName, AvailabilityReplicaConnectionState expectedState, int timeout)
        {
            for (int i = 0; i < timeout; i += 1000)
            {
                host.AvailabilityGroups[agName].AvailabilityReplicas[replicaName].Refresh();
                if (host.AvailabilityGroups[agName].AvailabilityReplicas[replicaName].ConnectionState == expectedState)
                {
                    return;
                }
                System.Threading.Thread.Sleep(1000);
            }
        }


        /// <summary>
        /// Delegate method to check whether AG is really joined.
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static bool IsAGJoined(Object[] input)
        {
            SMO.Server server = input[0] as SMO.Server;
            string agName = input[1] as string;

            server.AvailabilityGroups.Refresh();
            return (server.AvailabilityGroups.Contains(agName));
        }

        /// <summary>
        /// Delegate method for IsStateReached.
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static bool IsPrimaryReplica(Object[] input)
        {
            return IsPrimaryReplica(input[0] as string, input[1] as string, input[2] as SMO.Server);
        }

        /// <summary>
        /// Delegate method for IsStateReached.
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static bool IsReplicaSynchronized(Object[] input)
        {
            return IsReplicaSynchronized(input[0] as string, input[1] as string, input[2] as SMO.Server);
        }

        /// <summary>
        /// Switches all replicas within an availability group to SynchronousCommit state.
        /// </summary>
        /// <param name="availabilityGroupName">Availability group name</param>
        /// <param name="server">Name of server to check</param>
        static public void SetReplicasToSynchronousCommitMode(string availabilityGroupName, SMO.Server server)
        {
            AvailabilityReplicaCollection repCol = server.AvailabilityGroups[availabilityGroupName].AvailabilityReplicas;

            foreach (AvailabilityReplica replica in repCol)
            {
                if (replica.AvailabilityMode != AvailabilityReplicaAvailabilityMode.SynchronousCommit)
                {
                    replica.AvailabilityMode = AvailabilityReplicaAvailabilityMode.SynchronousCommit;
                    replica.Alter();


                }
            }
        }

        static public void SetSecondaryReplicasToSynchronousCommitMode(string availabilityGroupName, SMO.Server server)
        {
            AvailabilityReplicaCollection repCol = server.AvailabilityGroups[availabilityGroupName].AvailabilityReplicas;

            foreach (AvailabilityReplica replica in repCol)
            {
                if (replica.Role == AvailabilityReplicaRole.Secondary)
                {
                    if (replica.AvailabilityMode != AvailabilityReplicaAvailabilityMode.SynchronousCommit)
                    {
                        replica.AvailabilityMode = AvailabilityReplicaAvailabilityMode.SynchronousCommit;
                        replica.Alter();
                    }
                }
            }
        }


        /// <summary>
        /// Sets asynchronous commit mode on primary replica
        /// </summary>
        /// <param name="availabilityGroupName">Availability Group Name</param>
        /// <param name="server">Server hosting the replica</param>
        static public void SetPrimaryReplicaToAsynchronousCommitMode(string availabilityGroupName, SMO.Server server)
        {
            AvailabilityReplicaCollection repCol = server.AvailabilityGroups[availabilityGroupName].AvailabilityReplicas;

            foreach (AvailabilityReplica replica in repCol)
            {
                if (replica.Role == AvailabilityReplicaRole.Primary)
                {
                    if (replica.AvailabilityMode != AvailabilityReplicaAvailabilityMode.AsynchronousCommit)
                    {
                        replica.AvailabilityMode = AvailabilityReplicaAvailabilityMode.AsynchronousCommit;
                        replica.Alter();


                    }
                    break;
                }

                // TODO:Need to wait for secondary replica to switch to synchronizing state.
            }
        }
    }
}
