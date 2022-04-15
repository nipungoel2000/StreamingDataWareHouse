// var daemon = require('daemon');
// daemon.start();

const { execFile } = require('child_process');
execFile('./run_sdwh.sh', (error, stdout, stderr) => {
    if (error) {
        console.error(`error: ${error.message}`);
        // return res.status(500).send({message: "Internal Server Error"});
    }

    if (stderr) {
        console.error(`stderr: ${stderr}`);
        // return res.status(500).send({message: "Internal Server Error"});
    }
    // return res.status(200).send({message: "Shell script executed"});
    console.log(`stdout:\n${stdout}`);
});