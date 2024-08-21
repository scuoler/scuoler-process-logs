import { Pool, QueryResult } from "pg";
import { Stream } from "stream";

const pg = require("pg");
const configuration = require("../Configuration");
const constants = require("../Constants");
const fs = require("fs");

const path = require("path");
const fsPromises = require("fs/promises");
const readline = require("readline");


const getPool: () => Pool = () => {
    const pool: Pool = new pg.Pool({
        host: configuration.getHost(),
        user: configuration.getUserId(),
        password: configuration.getPassword(),
        database: configuration.getDatabase(),
        port: configuration.getPort(),
        ssl: { rejectUnauthorized: false },
    });

    return pool;
};

const processLogs = (direPath: string) => {
    const processSuccessFiles: string[] = [];
    const pool: Pool = getPool();
    fs.readdir(direPath, function (err: Error, files: any[]) {
        //handling error
        if (err) {
            return console.log("Unable to scan directory: " + err);
        }
        const pastReg: RegExp = /\d+-\d+-\d+-access.log$/;
        const curReg: RegExp = /access.log$/;
        //listing all files using forEach
        files.forEach(function (fileName) {
            //console.log(typeof fileName, fileName, pastReg.test(fileName));
            if (pastReg.test(fileName)) {
                processFile(direPath, fileName, processSuccessFiles, pool);
            }
            /*if (curReg.test(fileName)) {

            }*/

        });
    });
    //pool.end(() => { });

};


const processFile = (directoryPath: string, fileName: string, processedFiles: string[], pool: Pool) => {
    //console.log(directoryPath, fileName);

    let filePath = `${directoryPath}/${fileName}`;

    const readInterface: { input: Stream, output: null, console: boolean, on: Function } = readline.createInterface({
        input: fs.createReadStream(filePath),
        output: null,
        console: false,
    });

    readInterface.on("line", function (line: string) {
        processLine(line, fileName, pool);
    });
    readInterface.on("close", function () {
        processedFiles.push(fileName);
        let newPath: string = `${directoryPath}/processed/${fileName}`;
        fs.rename(filePath, newPath, function (err: Error) {
            if (err) throw err;

            console.log(`Successfully  ${fileName} moved!`);
        });
    });
};

const processLine = (line: string, fileName: string, pool: Pool) => {

    //\s = white space character, \S - negation class
    //
    let insertQuery: string =
        "insert into web_logs( " +
        " source_ip, " +
        " user_id," +
        " log_timestamp, " +
        " request_method_url," +
        " response_status," +
        " response_length," +
        " referrer," +
        " user_agent, " +
        " log_filename) values ( $1, $2, $3, $4, $5, $6, $7, $8, $9 )";
    let reg: RegExp =
        /\[(\S+)\]\s+\[(\S+)\]\s+\[(\S+\s\S+)\]\s+\[(\S+\s\S+)\]\s+\[(\S+)\]\s+\[(\S+)\]\s+\[(\S+)\]\s+\[(.+)\]$/;
    let matchResult: RegExpMatchArray | null = line.match(reg);
    if (matchResult?.length && matchResult?.length > 0) {
        let remoteIp = matchResult[1];
        let remoteUser = matchResult[2];
        let dateTime = matchResult[3];
        let methodAndUrl = matchResult[4].substring(0, 999);
        let status = matchResult[5];
        let responseLength = matchResult[6];
        let referrer = matchResult[7].substring(0, 999);
        let userAgent = matchResult[8].substring(0, 999);
        /*console.log(
            remoteIp,
            remoteUser,
            dateTime,
            methodAndUrl,
            status,
            responseLength,
            referrer,
            userAgent
        );*/
        pool
            .query(insertQuery, [
                remoteIp,
                remoteUser,
                dateTime,
                methodAndUrl,
                status,
                responseLength,
                referrer,
                userAgent,
                fileName,
            ])
            .then()
            .catch((err: Error) => {
                console.log(fileName, line, err);
            });
    } else {
        console.log("could not process", line);
    }
};

const updateLogViewsAndTables = () => {
    const pool: Pool = getPool();

    let selectQuery: string = `
    select * from public.web_logs_views_materialized()
    `

    pool.query(selectQuery, [],
        function (err: Error, result: QueryResult) {
            pool.end(() => { });
            if (err) {
                console.log(err);
            } else {
                console.log(result.rows);
            }
        });
}


const main = () => {
    processLogs(constants.LOG_DIRECTORY_PATH);
    updateLogViewsAndTables();
};

//cron.schedule("*/5 * * * *", main);

main();