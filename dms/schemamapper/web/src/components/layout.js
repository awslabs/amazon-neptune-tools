import { Navbar, Nav, NavDropdown, Form, FormControl, Button, Image, Container, Row, Col, Card, ListGroup } from 'react-bootstrap';
import Amplify, { Auth } from 'aws-amplify';
import React, { useState, useEffect } from 'react';
import BannerLogin from './bannerLogin';

import {
    BrowserRouter as Router,
    Switch,
    Route,
    // Link,
    Redirect,
    useHistory,
    useLocation
} from "react-router-dom";

const name = 'NeptuneUIUtils'
export const siteTitle = 'NeptuneUIUtils'
export default function Layout({ children, home, login }) {
    const history = useHistory()
    const [user, setUser] = useState({})

    function SignIn(e) {
        e.preventDefault()
        history.push('/login')
    }

    // useEffect(() => {
    //     Auth.currentAuthenticatedUser()
    //         .then(
    //             newuser => {
    //                 console.log(newuser.attributes.email);
    //                 setUser(user => {
    //                     return newuser.attributes.email
    //                 });
    //             }
    //         ).catch(err => console.log(err));
    // },[]);


    function showDetails(e) {
        window.location.href = "/"
    }
    return (
        <div>
            <div>
                <Navbar bg="dark" expand="lg" variant='bg-primary'>
                    <Container>
                        <Navbar.Brand href="/">
                            <Nav.Link href="/">
                                <Image
                                    priority
                                    src="/neptune.png"
                                    className="borderCircle"
                                    height={50}
                                    width={50}
                                    alt={name}
                                />
                            </Nav.Link>
                        </Navbar.Brand>
                        <Navbar.Toggle aria-controls="basic-navbar-nav" />
                        <Navbar.Collapse id="basic-navbar-nav">
                            <Nav className="me-auto">
                                {/* <Nav.Link href="/">Home</Nav.Link> */}
                                <Nav.Link href="/index.html">Schema-mapper</Nav.Link>
                                {/* <Nav.Link href="/gm">Graph-modeller</Nav.Link> */}
                                {/* <NavDropdown title="Dropdown" id="basic-nav-dropdown">
                                    <NavDropdown.Item href="#action/3.1">Action</NavDropdown.Item>
                                    <NavDropdown.Item href="#action/3.2">Another action</NavDropdown.Item>
                                    <NavDropdown.Item href="#action/3.3">Something</NavDropdown.Item>
                                    <NavDropdown.Divider />
                                    <NavDropdown.Item href="#action/3.4">Separated link</NavDropdown.Item>
                                </NavDropdown> */}
                            </Nav>
                        </Navbar.Collapse>
                    </Container>
                </Navbar>
            </div>
            <Container>
                <Row>
                    {/* <Col md={2}>
                        <Card>
                            <Card.Title><label>click to view details</label></Card.Title>
                            <Card.Body>
                                <ListGroup>
                                    <ListGroup.Item action onClick={e => showDetails(e)}>Connections</ListGroup.Item>
                                    <ListGroup.Item>Schemas</ListGroup.Item>
                                    <ListGroup.Item>Graphs</ListGroup.Item>
                                    <ListGroup.Item>Datasets</ListGroup.Item>
                                </ListGroup>
                            </Card.Body>
                        </Card>
                    </Col> */}
                    <Col>
                        <main>{children}</main>
                    </Col>
                </Row>
            </Container>
            {/* {!home && (
                <div className={styles.backToHome}>
                    <Link href="/">
                        <a>← Back to home</a>
                    </Link>
                </div>
            )} */}
            {/* <div className="charBotOverlay">
                <ChatBot
                    steps={[
                        {
                            id: '1',
                            message: 'What is your name?',
                            trigger: '2',
                        },
                        {
                            id: '2',
                            user: true,
                            trigger: '3',
                        },
                        {
                            id: '3',
                            message: 'Hi {previousValue}, nice to meet you!',
                            end: true,
                        },
                    ]}
                />
            </div> */}
            <hr />
            <footer class="footer">
                <div class="container">
                    contact <a href="mailto:neptune-developer-feedbac@amazon.com">wwso-neptune-ssa@amazon.com</a>for any feedback or questions
                </div>
            </footer>

        </div>
    )
}


Amplify.configure({
    Auth: {

        // REQUIRED only for Federated Authentication - Amazon Cognito Identity Pool ID
        identityPoolId: 'XX-XXXX-X:XXXXXXXX-XXXX-1234-abcd-1234567890ab',

        // REQUIRED - Amazon Cognito Region
        region: 'us-west-2',

        // OPTIONAL - Amazon Cognito Federated Identity Pool Region 
        // Required only if it's different from Amazon Cognito Region
        identityPoolRegion: 'XX-XXXX-X',

        // OPTIONAL - Amazon Cognito User Pool ID
        userPoolId: 'us-west-2_OWNcr25Bc',

        // OPTIONAL - Amazon Cognito Web Client ID (26-char alphanumeric string)
        userPoolWebClientId: '5bqevntpu9os55uuppkpuupfma',

        // OPTIONAL - Enforce user authentication prior to accessing AWS resources or not
        mandatorySignIn: false,

        // OPTIONAL - Configuration for cookie storage
        // Note: if the secure flag is set to true, then the cookie transmission requires a secure protocol
        // cookieStorage: {
        // // REQUIRED - Cookie domain (only required if cookieStorage is provided)
        //     domain: '.yourdomain.com',
        // // OPTIONAL - Cookie path
        //     path: '/',
        // // OPTIONAL - Cookie expiration in days
        //     expires: 365,
        // // OPTIONAL - See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Set-Cookie/SameSite
        //     sameSite: "strict" | "lax",
        // // OPTIONAL - Cookie secure flag
        // // Either true or false, indicating if the cookie transmission requires a secure protocol (https).
        //     secure: true
        // },

        // OPTIONAL - customized storage object
        //storage: MyStorage,

        // OPTIONAL - Manually set the authentication flow type. Default is 'USER_SRP_AUTH'
        authenticationFlowType: 'USER_PASSWORD_AUTH',

        // OPTIONAL - Manually set key value pairs that can be passed to Cognito Lambda Triggers
        clientMetadata: { myCustomKey: 'myCustomValue' },

        // OPTIONAL - Hosted UI configuration
        oauth: {
            domain: 'your_cognito_domain',
            scope: ['phone', 'email', 'profile', 'openid', 'aws.cognito.signin.user.admin'],
            redirectSignIn: 'http://localhost:3000/',
            redirectSignOut: 'http://localhost:3000/',
            responseType: 'code' // or 'token', note that REFRESH token will only be generated when the responseType is code
        }
    }
});

// You can get the current config object
const currentConfig = Auth.configure();
