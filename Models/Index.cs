namespace ExportSqlCE
{
    /// <summary>
    /// TABLE_NAME, INDEX_NAME, PRIMARY_KEY, [UNIQUE], [CLUSTERED], ORDINAL_POSITION, COLUMN_NAME, COLLATION as SORT_ORDER
    /// </summary>
    struct Index
    {
        public string TableName { get; set; }
        public string IndexName { get; set; }
        public bool Unique { get; set; }
        public bool Clustered { get; set; }
        public int OrdinalPosition { get; set; }
        public string ColumnName { get; set; }
        public SortOrderEnum SortOrder { get; set; }

    }


    enum SortOrderEnum
    {
        Asc
        , Desc
    }
}
