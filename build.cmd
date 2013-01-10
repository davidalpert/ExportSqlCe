call rebuildallexceptsetup.cmd
cd lib
call ilmexpsqlce.cmd
del c:\data\sqlce\exp*.zip
del c:\data\sqlce\sqlce*.zip
"c:\program files\7-zip\7z" a c:\data\sqlce\exportsqlce.zip c:\data\sqlce\exportsqlce.exe
"c:\program files\7-zip\7z" a c:\data\sqlce\exportsqlce40.zip c:\data\sqlce\exportsqlce40.exe
"c:\program files\7-zip\7z" a c:\data\sqlce\exportsqlce31.zip c:\data\sqlce\exportsqlce31.exe
"c:\program files\7-zip\7z" a c:\data\sqlce\export2sqlce.zip c:\data\sqlce\export2sqlce.exe
"c:\program files\7-zip\7z" a c:\data\sqlce\SqlCeScripting.zip c:\data\sqlce\*.dll
cd ..
cd ..
dir *.zip
pause