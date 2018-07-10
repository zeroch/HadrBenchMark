using Microsoft.VisualStudio.TestTools.UnitTesting;
using HadrBenchMark;
using System.Data;
using System;
namespace TestHadrBenchMark
{
    [TestClass]
    public class TestHadrTestBase
    {
        HadrTestBase m_hadr;
        [TestInitialize]
        public void TestInit()
        {
            m_hadr = new HadrTestBase();
            m_hadr.Setup();
            // simulate 10 database
            m_hadr.primaryDbsNames = new System.Collections.Generic.List<string>()
            {
                "DB_1",
                "DB_2",
                "DB_3",
                "DB_4",
                "DB_5",
                "DB_6",
                "DB_7",
                "DB_8",
                "DB_9",
                "DB_10",
                "DB_11",
                "DB_12",
                "DB_13",
                "DB_14",
                "DB_15",
                "DB_16",
                "DB_17",
                "DB_18",
                "DB_19",
                "DB_20"
            };
        }
        [TestMethod]
        public void TestInsertFailoverReport()
        {
            m_hadr.InsertFailoverReport(DateTime.Now, DateTime.Now, 200);
        }
        [TestMethod]
        public void TestPartialTraffic()
        {
            m_hadr.StartPartialTraffic();
        }


        [TestMethod]
        public void TestFullTraffic()
        {

            m_hadr.StartFullTraffic();
        }
    }
}
