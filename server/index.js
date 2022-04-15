const express = require('express');
const app = express();
const cors = require("cors");
const shell = require('shelljs')
// const mysql = require('mysql');

// const conn = mysql.createConnection(
//     {
//         host: "localhost",
//         user: "root",
//         password : "root",
//         database: "stdwh_db"
//     }
// );
require('dotenv').config();

//CONNECT TO MYSQL DATABASE;

app.use(express.json());
app.use(cors());

const port = 9001;

app.get('/execute', (req,res) =>
{
    shell.exec('./../run_sdwh.sh');
})
// app.get('/getEntryPoints',(req,res)=>
// {

// })

// app.post('/query',(req,res)=>
// {

// })
// const entryPoints = require("./routes/")
app.listen(port, ()=>{
    console.log("Server started on port"+ `${port}`);
});