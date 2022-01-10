const {getHeaders} = require("../neptune-gremlin.js")

test("getHeaders", () => {

    const expected = {
        Host: "myneptunecluster.us-east-1.neptune.amazonaws.com:8182",
        "X-Amz-Security-Token": "................",
        "X-Amz-Date": "20211123T191311Z",
        Authorization: "AWS4-HMAC-SHA256 Credential=.../20211123/us-east-1/neptune-db/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token, Signature=...",
    }

    const headers = getHeaders(
        "myneptunecluster.us-east-1.neptune.amazonaws.com",
        8182,
        {
            accessKey: "...",
            secretKey: "...",
            sessionToken: "AAAAAA1111111",
            region: "us-east-1",
        },
        "/gremlin")

    console.log(getHeaders)

    expect(headers.Host).toEqual(expected.Host)
    expect(headers["X-Amz-Security-Token"]).toBeTruthy() // ?
    expect(headers["X-Amz-Date"].length).toEqual(16)
    expect(headers.Authorization.indexOf("AWS4-HMAC-SHA256 Credential=")).toEqual(0)

})