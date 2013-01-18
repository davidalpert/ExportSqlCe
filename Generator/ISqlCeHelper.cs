﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ErikEJ.SqlCeScripting
{
    public enum SQLCEVersion
    {
        SQLCE20 = 0,
        SQLCE30 = 1,
        SQLCE35 = 2,
        SQLCE40 = 3
    }

    public interface ISqlCeHelper
    {
        string FormatError(Exception ex);
        string GetFullConnectionString(string connectionString);
        void CompactDatabase(string connectionString);
        void CreateDatabase(string connectionString);
        void VerifyDatabase(string connectionString);
        void RepairDatabaseRecoverAllPossibleRows(string connectionString);
        void RepairDatabaseRecoverAllOrFail(string connectionString);
        void RepairDatabaseDeleteCorruptedRows(string connectionString);
        void ShrinkDatabase(string connectionString);
        string PathFromConnectionString(string connectionString);
        void UpgradeTo40(string connectionString);
        SQLCEVersion DetermineVersion(string fileName);
        bool IsV35Installed();
        bool IsV40Installed();
    }
}
