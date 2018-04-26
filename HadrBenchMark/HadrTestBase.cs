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
        private int increaseDBCount = 200;

        public List<string> primaryDbsNames;
        public List<Smo.Database> primaryDbs;

        public void Setup()
        {
            agName = "HadrBenchTest";
            //primaryServerName = @"zecheBenchTest01\hadrBenchMark01";
            //secondaryServerName = @"ze-bench-02\hadrBenchMark01";
            primaryServerName = @"ze-2016-v1\sql16rtm01";
            secondaryServerName = @"ze-2016-v2\sql16rtm01";
            // start point of dbCount, lets say 500
            primaryDbsNames = new List<string>();
            dbCount = 500;
            for(int i = 1; i < dbCount; i++)
            {
                primaryDbsNames.Add(string.Format("DB_{0}", i));
            }

            // get connection to primary and secondary server
            primary = new Smo.Server(primaryServerName);
            secondary = new Smo.Server(secondaryServerName);

            this.primaryEndpointUrl = ARHelper.GetHadrEndpointUrl(primary);
            this.secondaryEndpointUrl = ARHelper.GetHadrEndpointUrl(secondary);

            Console.WriteLine("Create hadrEnpoint Url: {0}", primaryEndpointUrl);
            Console.WriteLine("Create hadrEnpoint Url: {0}", secondaryEndpointUrl);
        }

        public void CleanUp()
        {
            AGHelper.DropAG(agName, primary);
        }

        public void TestNodesHADREnabled()
        {
            bool primaryEnabled = primary.IsHadrEnabled;
                Console.WriteLine("The server: {0} is {1} enabled HADR", primary.Name, primaryEnabled ? "" : "not" );

            bool secondaryEnabled = secondary.IsHadrEnabled;
                Console.WriteLine("The server: {0} is {1} enabled HADR", secondary.Name, secondaryEnabled ? "" : "not");

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
                Console.WriteLine("Creating availability group '{0}' on server '{1}', with replica on server '{2}' and no databases.",
                                            ag.Name, primary.Name, secondary.Name);

                ag.Create();
                Thread.Sleep(1000); //Sleep a tick to let AG create take effect

                secondary.JoinAvailabilityGroup(agName);
                // enable autoseeding in secondary
                secondary.GrantAvailabilityGroupCreateDatabasePrivilege(agName);

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Failed to create AG {0}", ag.Name);
                AGHelper.DropAG(ag.Name, primary);
            }
        }


    }
}
