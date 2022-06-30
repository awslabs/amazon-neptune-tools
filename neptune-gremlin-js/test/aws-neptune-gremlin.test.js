const {getHeaders, getAws4Headers} = require("../neptune-gremlin.js")

test("getHeaders", async () => {

    const expected = {
        Host: "myneptunecluster.us-east-1.neptune.amazonaws.com:8182",
        "X-Amz-Security-Token": "................",
        "X-Amz-Date": "20211123T191311Z",
        Authorization: "AWS4-HMAC-SHA256 Credential=.../20211123/us-east-1/neptune-db/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token, Signature=...",
    }

    const headers = await getHeaders(
        "myneptunecluster.us-east-1.neptune.amazonaws.com",
        8182,
        {
            accessKey: "...",
            secretKey: "...",
            sessionToken: "AAAAAA1111111",
            region: "us-east-1",
        },
        "/gremlin")

    console.log(headers)

    expect(headers.host).toEqual(expected.Host)
    expect(headers["x-amz-security-token"]).toBeTruthy() // ?
    expect(headers["x-amz-date"].length).toEqual(16)
    expect(headers.authorization.indexOf("AWS4-HMAC-SHA256 Credential=")).toEqual(0)

})

test("compare sigs", async () => {

    const host = "myneptunecluster.us-east-1.neptune.amazonaws.com"
    const port = 8182
    const creds = {
        accessKey: "...",
        secretKey: "...",
        sessionToken: "AAAAAA1111111",
        region: "us-east-1",
    }
    const path = "/gremlin"
    const headers = await getHeaders(host, port, creds, path)

    console.log({headers})

    const aws4headers = getAws4Headers(host, port, creds, path)

    console.log({aws4headers})

    //expect(headers.authorization).toEqual(aws4headers.Authorization)
})
