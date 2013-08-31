
echo Running ilmerge...
c:\data\sqlce\exportsqlce\lib\ilmerge /targetplatform:"v4,C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.0" /out:c:\data\sqlce\ExportSqlCe.exe ..\bin\release\exportsqlce.exe QuickGraph.dll QuickGraph.Data.dll QuickGraph.GraphViz.dll

c:\data\sqlce\exportsqlce\lib\ilmerge /targetplatform:"v4,C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.0" /out:c:\data\sqlce\ExportSqlCe40.exe ..\bin\release\exportsqlce40.exe QuickGraph.dll QuickGraph.Data.dll QuickGraph.GraphViz.dll

c:\data\sqlce\exportsqlce\lib\ilmerge /targetplatform:"v4,C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.0" /out:c:\data\sqlce\ExportSqlCe31.exe ..\bin\x86\release\exportsqlce31.exe QuickGraph.dll QuickGraph.Data.dll QuickGraph.GraphViz.dll

c:\data\sqlce\exportsqlce\lib\ilmerge /targetplatform:"v4,C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.0" /out:c:\data\sqlce\Export2SqlCe.exe ..\bin\release\export2sqlce.exe QuickGraph.dll QuickGraph.Data.dll QuickGraph.GraphViz.dll

del c:\data\sqlce\exp*.pdb

echo Copying new Scripting API files...

copy C:\Data\SQLCE\exportsqlce\bin\Release\*.dll c:\data\sqlce 

pause