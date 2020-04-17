#!/bin/sh
file_name=$1
sour=\``head -n 3 ${file_name} | tail -1 | awk -F: '{ sub(/^[[:blank:]]*/,"",$3);print $3}'`\`.
sour2=`head -n 3 ${file_name} | tail -1 | awk -F: '{ sub(/^[[:blank:]]*/,"",$3);print $3}'`.
echo ${sour}....${sour2}
des=''

#replace view `dbname`. is '' 
sed -i s/${sour}/${dest}/g ${file_name}
sed -i s/${sour2}/${dest}/g ${file_name}
sed -i s/DEFINER=.*DEFINER//g ${file_name}

#replace procdure 
sed -i s/DEFINER=.*PROCEDURE/PROCEDURE/g ${file_name}

#replace function
sed -i s/DEFINER=.*FUNCTION/FUNCTION/g ${file_name}

#replace trigger
old="^.*CREATE.*DEFINER.*TRIGGER "
new="CREATE \/\*\!50003 TRIGGER "
sed -i s/"${old}"/"${new}"/g ${file_name}

#replace event
old="^.*CREATE.*DEFINER.*EVENT "
new="CREATE \/\*\!50106 EVENT "
sed -i s/"${old}"/"${new}"/g ${file_name}
