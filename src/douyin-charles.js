/**
 *
 * 解析charles获取的数据包
 * 1. 一个charles JSON Session File(.chlsj)包含当前domain所有requests, 第一步先把所有requests解析出来
 * 2. 一个request包含6个videos信息, 所以第二部把所有videos解析出来
 *
 * 3. 把视频解析出来之后, 发给consulting项目组(Chen Jingyu)
 *
 */
'use strict';
const fs = require('fs');
const path = require('path');
const moment = require('moment');
const logger = require('winston');
const cityList = require('./citymap');

if (process.argv.length < 3) {
    logger.error('node douyin.charles.js <fpath>');
    logger.error('node douyin.charles.js D:/douyin/');
    process.exit();
}

let resultDir = '../result/';
let videoFile = `${resultDir}douyin.videos_${moment().format('YYYY-MM-DD')}.csv`;
let userIdFile = `${resultDir}douyin.users_${moment().format('YYYY-MM')}.csv`;
let userSet = new Set();
start();

function start() {
    init();
    parser();
}

function parser() {
    //Timer reset for each json seesion package
    let currentCity = '';
    let firstCityPackageTime;
    let _parser = fpath => {
        let buffer = fs.readFileSync(fpath);
        let resultObjs = JSON.parse(buffer);

        logger.info(`File: ${fpath} - ${resultObjs.length} 个请求`);

        resultObjs.forEach((reqObjs, idx) => {

            //this part is used to extract the cityName from request all response wihout valid longitude and latitude will be skipped
            const startTime = reqObjs.times.start;
            const queryString = reqObjs.query;
            let parametersList = queryString.split('&');
            let cityKey = '';
            parametersList.forEach(parameter => {
                let pKey = parameter.split('=')[0];
                let pValue = parameter.split('=')[1];
                if (pKey === 'longitude' || pKey === 'latitude') {
                    cityKey = cityKey.concat(pValue.slice(0, pValue.indexOf('.') + 2));
                }
            })

            let cityName = cityList.get(cityKey);
            if (!cityName) {
                console.log(`*************************${cityKey}`)
            }
            if (cityName) {
                let offsetTimeFromFirstPackage;
                if (currentCity !== cityName) {
                    //first package with valid city name
                    currentCity = cityName;
                    firstCityPackageTime = moment(startTime);
                    offsetTimeFromFirstPackage = '0:00:00';
                } else {
                    offsetTimeFromFirstPackage = getTimeOffSetString(moment(startTime).diff(moment(firstCityPackageTime)));
                }
                if (!reqObjs.response.body) {
                    logger.warn(`      ${fpath} - 第 ${idx} 个, response为空`);
                    return;
                }
                if (reqObjs.path !== '/aweme/v1/feed/') {
                    logger.warn(`      ${fpath} 跳过 - 第 ${idx} 个, ${reqObjs.path}`);
                    return;
                }
                if (reqObjs.response.body.encoding === 'base64') {
                    reqObjs.response.body.text = new Buffer(reqObjs.response.body.encoded, 'base64').toString();
                }
                try {
                    reqObjs = JSON.parse(reqObjs.response.body.text).aweme_list;
                    //console.log(reqObjs.request);
                } catch (err) {
                    logger.error(`      ${fpath} - 第 ${idx} 个, 解析错误`);
                    return;
                }
                if (!reqObjs) {
                    logger.warn(`      ${fpath} - 第 ${idx} 个, aweme_list列表为空`);
                    return;
                }

                reqObjs.forEach(videoObj => {
                    let row = [
                        videoObj.aweme_id,
                        videoObj.desc,
                        moment(Number(videoObj.create_time + '000')).format('YYYY-MM-DD'),
                        videoObj.author.nickname,
                        videoObj.author.uid,
                        moment(Number(videoObj.author.create_time + '000')).format('YYYY-MM-DD'),
                        videoObj.author.constellation,
                        videoObj.author.birthday,
                        videoObj.author.gender,
                        videoObj.share_url,
                        videoObj.is_ads,
                        cityName,
                        moment(startTime).format('YYYY-MM-DD'),
                        moment(startTime).format('HH:MM:ss'),
                        offsetTimeFromFirstPackage
                    ].map(item => {
                        item += '';
                        return item.trim().replace(/[\s,"]+/g, ' ');
                    }).join();
                    fs.appendFileSync(videoFile, row + '\n');
                    if (userSet.has(videoObj.author.uid)) return;
                    if (userSet.size > 40000) return;
                    // fs.appendFileSync(userIdFile, videoObj.author.uid + '\n');
                    userSet.add(videoObj.author.uid);
                });
            }
        });

    };

    getFiles(process.argv[2]).map(_parser);
    logger.info(`Done: ${userSet.size} 个用户`);
}

function getTimeOffSetString(miliSeconds) {
    let rawSeconds = Math.floor(miliSeconds / 1000);
    let hourNumber = Math.floor(rawSeconds / 3600);
    rawSeconds = rawSeconds - hourNumber * 3600;
    let minNumber = Math.floor(rawSeconds / 60);
    rawSeconds = rawSeconds - minNumber * 60;
    let secNumber = rawSeconds;
    const hourString = hourNumber.toString();
    const minString = minNumber < 10 ? `0${minNumber}` : minNumber.toString();
    const secString = secNumber < 10 ? `0${secNumber}` : secNumber.toString();

    return `${hourString}:${minString}:${secString}`
}

function init() {
    if (!fs.existsSync(resultDir)) fs.mkdirSync(resultDir);
    fs.writeFileSync(videoFile, '\ufeff视频ID,描述,上传日期,作者,作者ID,注册日期,星座,生日,性别,URL,广告,请求城市,请求数据包发送日期,请求数据包发送时间,距离第一个请求时间\n');
    // turn off user file write
    // fs.writeFileSync(userIdFile, '\ufeff');
}

/**
 *
 * 返回给定路径下所有文件，包括子目录
 *
 */
function getFiles(fpath, files) {
    files = files || [];
    let isDir = fname => fs.statSync(fname).isDirectory();

    fs.readdirSync(fpath).forEach((fname, idx) => {
        let subFpath = path.join(fpath, fname);
        if (isDir(subFpath)) return getFiles(subFpath, files);
        files.push(subFpath);
    });
    return files;
}