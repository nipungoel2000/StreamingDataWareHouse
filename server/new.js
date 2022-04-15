var fs = require('fs');
var parser = require('xml2js');
var parseString = parser.parseString;

function getEntryPoints(XMLfname)
{
    var entryPoints = [];
    const data = fs.readFileSync(XMLfname)
    
    parseString(data, function (err, result) {
        if(err)
            console.log(err);

        var dims = result['sdwh-schema']['dimensions'][0]['dim'];
        
        for(var i=0; i<dims.length; i++)
        {    
            for(var j=0; j<dims[i]["field"].length; j++)
            {
                // console.log(JSON.stringify(dims[i]["field"][j]['$']['is-EntryPoint']));
                var is_ep = JSON.stringify(dims[i]["field"][j]['$']['is-EntryPoint']);
                // console.log(is_ep.localeCompare("\"true\""));
                if(is_ep == "\"true\"")
                {   
                    var col_name = JSON.stringify(dims[i]["field"][j]['name'][0]);
                    console.log(col_name.substring(1,col_name.length-1));
                    entryPoints.push(col_name.substring(1,col_name.length-1))
                    // console.log("OK");
                }
            }
        }
        // return entryPoints;
        })
        return entryPoints;
};

var list = getEntryPoints("config_v2.xml");
console.log(list);



var sqlstmt = "select MAX(tick) from ";
sqlstmt += tableName;
console.log(tableName);
conn.query(sqlstmt,function(err,result)
{
    if(err)
    {
        console.log(err);
        return res.status(400).send({"message": "Fail"});
    }
    console.log(result);
    return res.status(200).send({"message":"Success", "tickNumber": tick});
})