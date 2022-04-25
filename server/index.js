const express = require('express');
const app = express();
const cors = require("cors");
const shell = require('shelljs');
const { status, json } = require('express/lib/response');
var fs = require('fs');
var parser = require('xml2js');
var parseString = parser.parseString;

const mysql = require('mysql2/promise');
const { table } = require('console');

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
                    console.log("dim_" + dims[i]['$']['name'] + "_" + col_name.substring(1,col_name.length-1));
                    entryPoints.push("dim_" + dims[i]['$']['name'] + "_" + col_name.substring(1,col_name.length-1))
                    // console.log("OK");
                }
            }
        }
        // return entryPoints;
        })
        return entryPoints.sort();
};

var entryPoints = getEntryPoints("config_v2.xml");

// parseString(xml, function (err, result) {
//     console.dir(result);
// });


app.use(express.json());
app.use(cors());

const port = 9001;

app.get('/execute', (req,res) =>
{
    // shell.exec('./run_sdwh.sh');
    // const { execFile } = require('child_process');
    // execFile('./run_sdwh.sh', (error, stdout, stderr) => {
    // if (error) {
    //     console.error(`error: ${error.message}`);
    //     return res.status(500).send({message: "Internal Server Error"});
    // }
    
    // if (stderr) {
    //     console.error(`stderr: ${stderr}`);
    //     return res.status(500).send({message: "Internal Server Error"});
    // }
    // return res.status(200).send({message: "Shell script executed"});
    // console.log(`stdout:\n${stdout}`);
    // });  
    const spawn = require('child_process').spawn,
    test2 = spawn('node', ['test2.js']);
    console.log(test2.pid);
    return res.status(200).send({message: "Shell script executed"});
})
app.get('/getEntryPoints',(req,res)=>
{   
    return res.status(200).send({"EntryPoints" : entryPoints});
})

app.get('/getTick', async (req,res)=>
{   
    console.log("Here");
    try {
        const conn = await mysql.createConnection(
            {
                host: "localhost",
                user: "root",
                password : "root",
                database: "stdwh_db"
            }
        );
        var len = entryPoints.length;
        var tableName = "mv";
        for(var i=0; i<len; i++)
            tableName += "0";
        var sqlstmt = "select MAX(tick) as tick from ";
        sqlstmt += tableName;
        let [rows, fields] = await conn.execute(sqlstmt);
        // console.log([rows,fields]);
        return res.status(200).send({"message":"Success","tickNum":rows[0].tick});
        // await conn.connect(async function(err)
        // {
        //     if(err) throw err;
        //     console.log("Connected!");
        //     var len = entryPoints.length;
        //     var tableName = "mv";
        //     for(var i=0; i<len; i++)
        //         tableName += "0";
        //     var sqlstmt = "select MAX(tick) from ";
        //     sqlstmt += tableName;
        //     console.log(tableName);
        //     var result2;
        //     await conn.query(sqlstmt,function(err,result)
        //     {
        //         if(err)
        //         {
        //             console.log(err);
        //             return res.status(400).send({"message": "Fail"});
        //         }
        //         console.log(result[0]);
        //         result2=JSON.stringify(result);
        //         // return response
        //         // return res.status(200).send({"message":"Success", "tickNumber": result});
        //     })
        //     return res.status(200).send({"message":"Success", "tickNumber": result2});
        // })
    } catch (error) {
        return res.status(500).send({"message":error});
    }
    
})
app.post('/query',async (req,res)=>
{
    entryPoints.sort();
    var points=req.body.dimensions;
    console.log("Here In query");
    try {
        const conn = await mysql.createConnection(
            {
                host: "localhost",
                user: "root",
                password : "root",
                database: "stdwh_db"
            }
        );
        var len = entryPoints.length;
        var tick = req.body.tickNumber; //convert to string
        var tableName = "mv";
        console.log(points[0]);
        var temp=Array(entryPoints.length).fill(0);
        // console.log(temp);
        for(var i=0;i<points.length;i++)
        {   
            console.log(points[i]);
            var id=entryPoints.indexOf(points[i]);
            console.log(id);
            temp[id]=1;
            // console.log(points[i]);
        }
        for(var i=0;i<temp.length;i++)
        {
            tableName+=temp[i].toString();
        }
        var sqlstmt = "select * from " + tableName +" where tick = " + tick.toString();
        console.log(sqlstmt);
        let [rows, fields] = await conn.execute(sqlstmt);
        // console.log([rows,fields]);
        return res.status(200).send({"message":"Success","result":rows});
        // conn.connect(function(err)
        // {
        //     if(err) throw err;
        //     console.log("Connected!");
        //     var len = entryPoints.length;
        //     var tick = req.body.tickNumber; //convert to string
        //     var tableName = "mv";
        //     // console.log(points[0]);
        //     var temp=Array(entryPoints.length).fill(0);
        //     // console.log(temp);
        //     for(var i=0;i<points.length;i++)
        //     {   
        //         console.log(points[i]);
        //         var id=entryPoints.indexOf(points[i]);
        //         console.log(id);
        //         temp[id]=1;
        //         // console.log(points[i]);
        //     }
        //     for(var i=0;i<temp.length;i++)
        //     {
        //         tableName+=temp[i].toString();
        //     }
        //     var sqlstmt = "select * from " + tableName +" where tick = " + tick.toString();
        //     console.log(sqlstmt);
        //     // sqlstmt += tableName;
        //     console.log(tableName);
        //     conn.query(sqlstmt,function(err,result)
        //     {
        //         if(err)
        //         {
        //             console.log(err);
        //             return res.status(400).send({"message": "Fail"});
        //         }
        //         console.log(result);
        //         // return response
        //     })
        //     return res.status(200).send({"message":"Success"});//, "tickNumber": tick});
        // })
    } catch (error) {
        return res.status(500).send({"message":error});
    }
})
// const entryPoints = require("./routes/")
app.listen(port, ()=>{
    console.log("Server started on port"+ `${port}`);
});