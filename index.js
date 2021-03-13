const AWS = require('aws-sdk');
var crypto = require("crypto");


AWS.config.update({
    region: 'us-east-1'
})

function newId(){
    return crypto.randomBytes(20).toString('hex');
}

function parseDate(dateField) {
    if(!dateField) {
        return undefined
    }
    const dateObject = dateField.$date
    if (typeof dateObject == 'string') {
        return dateObject
    } else {
        return new Date(parseInt(dateObject.$numberLong)).toISOString()
    }
}

async function executeMigration(){ 
    console.log('Init migration')
    const dynamoDB = new AWS.DynamoDB.DocumentClient();
    const s3 = new AWS.S3()
    const data = await s3.getObject({Bucket:'church-members', Key:'database/backup/disciples/member/data.json'}).promise()
    const disciples = JSON.parse(data.Body.toString())
    const members = []
    const religionInfo = []
    disciples.forEach(member => {
        const memberParsed = {
            id: newId(),
            active: member.active,
            ...member.person,
            ...member.person.contact,
            ...member.person.address,
            birthDate: parseDate(member.person.birthDate),
            marriageDate: parseDate(member.person.marriageDate),
            adressNumber: member.person.address.number,
            name: `${member.person.firstName} ${member.person.lastName}`
        }
        const churchInfoParse = {
            id: newId(),
            memberId: memberParsed.id,
            ...member.religion,
            attendsSaturdayWorship: member.attendsSaturdayWorship,
            attendsSundayWorship: member.attendsSundayWorship,
            attendsSundaySchool: member.attendsSundaySchool,
            baptismDate: parseDate(member.religion.baptismDate),
            acceptedJesusDate: parseDate(member.religion.acceptedJesusDate),
        }
        delete memberParsed._id
        delete memberParsed.className
        delete memberParsed.number
        delete memberParsed.contact
        members.push(memberParsed)
        religionInfo.push(churchInfoParse)
    });
    for (let index = 0; index < members.length; index++) {
        const member = members[index];
        const religion = religionInfo[index]

        console.log(`Adding member ${member.id}`)
        let result = await dynamoDB.put({
            TableName: "member",
            Item: member,
        }).promise()
        if (result.error) {
            console.error("Unable to add member. Error JSON:", JSON.stringify(result.error, null, 2));
        } else {
            console.log(`Member ${member.id} inserted`)
        }
        result = await dynamoDB.put({
            TableName: "religionInfo",
            Item: religion,
        }).promise()
        if (result.error) {
            console.error("Unable to add religion. Error JSON:", JSON.stringify(result.error, null, 2));
        } else {
            console.log(`Member ${member.id} inserted`)
        }
    }
}


exports.handler = async (event) => {
    console.log('Init lambda')
    await executeMigration();
    const response = {
        statusCode: 200,
        body: JSON.stringify('Hello from Lambda!'),
    };
    return response;
};
