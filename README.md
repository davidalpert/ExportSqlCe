ExportSqlCe
===========

A mirror of [ErikEJ](http://www.codeplex.com/site/users/view/ErikEJ)'s SQL Compact data and schema script utility from CodePlex: http://exportsqlce.codeplex.com/


## RELEASE NOTES

This release contains 6 downloadable files:

* **ExportSqlCE_Addin.zip** - SSMS 2008 scripting add-in (for SQL Server Compact 3.5) 

* **Export2SqlCE.zip** - SQL Server 2005/2008 command line utility to generate a SQL Compact compatible script with schema and data (or schema only)

* **ExportSqlCE40.zip** - SQL Compact 4.0 command line utility to generate a script with schema and data

* **ExportSqlCE.zip** - SQL Compact 3.5 command line utility to generate a script with schema and data

* **ExportSqlCE31.zip** - SQL Compact 3.1 command line utility to generate a script with schema and data 

* **SqlCeScripting.zip** - SqlCeScripting .NET library (for SQL Server Compact 3.5 and 4.0), for easy inclusion of scripting in your own application

Improved SSMS add-in:

* Using latest scripting API

New and improved features in command line utilities:

- ability to exclude tables from being scripted - "exclude" switch
- scripting of Windows Phone Linq to SQL DataContext - "wpdc" switch
- new "dataonly" command line switch
- "dgml" script now contains object descriptions
- SQL Server Export: support for HierarchyId

##### Changes in release 3.5.2.9:

- BREAKING: All command line utilities now depend on .NET 4.0
- BREAKING: All Solution upgraded to VS 2010
- Tables are now sorted by topological sort
- Fix: Improved script comments handling
- Fix: DataContext - Avoiding duplicate indexes on Primary Keys

##### Changes in release 3.5.2.14:

- Improved handling of tables with . in their name
- DGML: Inlcudes schema name, no .sql files generated
- date and datetime2 always converted to datetime
