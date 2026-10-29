import {
    Navbar, Nav, NavDropdown, Form, FormControl, Button, Card, Container, Row, Col, ListGroup, Dropdown, FloatingLabel, Image, Link
} from 'react-bootstrap';
import React, { useState, useEffect } from 'react';
import { Auth } from 'aws-amplify';

import {
    BrowserRouter as Router,
    Switch,
    Route,
    // Link,
    Redirect,
    useHistory,
    useLocation
} from "react-router-dom";

import Layout from './layout';

export default function SignUpOrSignIn() {
    //check whether use has signed in or not and return accordingly

    const [email, setEmail] = useState('user@domain.com');
    const [password, setPassword] = useState('password');
    const [confirmpassword, setConfirmpassword] = useState('confirm password');
    const [registered, setRegistered] = useState(true);
    const [registeredPendingVerification, setRegisteredPendingVerfication] = useState(false);
    const [signedIn, setSignedIn] = useState(false);
    const [code, setCode] = useState('')
    const history = useHistory();


    async function SignUp() {
        var profile = {
            username: email,
            password: password,
            attributes: {
                email: email
            }
        };

        // alert(JSON.stringify(profile));

        //call amplify apis to create a new user
        try {
            const { user } = await Auth.signUp(profile);
            setRegisteredPendingVerfication(true)
            setRegistered(true);
            console.log(user);
        } catch (error) {
            console.log('error signing up:', error);
        }
    }

    async function SignIn() {
        var profile = {
            username: email,
            password: password
        };

        // alert(JSON.stringify(profile));
        //call amplify apis to sign in 

        try {
            const user = await Auth.signIn(email, password);
            setSignedIn(true);
            history.push('/')
        } catch (error) {
            console.log('error signing in', error);
        }
    }

    async function VerifyCode() {
        try {
            await Auth.confirmSignUp(email, code);
            setRegisteredPendingVerfication(false);
            setRegistered(true);
        } catch (error) {
            console.log('error confirming sign up', error);
        }
    }

    function handleEmailChange(e) {
        setEmail(e.target.value)
    }

    function handleEmailChange(e) {
        setEmail(e.target.value)
    }

    function handlePasswordChange(e) {
        setPassword(e.target.value)
    }

    function handleConfirmPasswordChange(e) {
        setConfirmpassword(e.target.value)
    }

    function handleCodeChange(e) {
        setCode(e.target.value)
    }

    function resetToSignUpView() {
        setRegistered(false);
    }

    function resetToSignInView() {
        setRegistered(true);
    }

    if (!registered) {
        return (
            <Layout>
                <Container className="text-center" fluid="md" id="loginContainer">
                    <Form id="loginForm">
                        <Form.Group as={Row} className="mb-3" controlId="formPlaintextEmail">
                            <Form.Label column sm="4">
                                Email
                            </Form.Label>
                            <Col sm="8">
                                <Form.Control type="text" name="email" placeholder={email} onChange={handleEmailChange} />
                            </Col>
                        </Form.Group>

                        <Form.Group as={Row} className="mb-3" controlId="formPlaintextPassword">
                            <Form.Label column sm="4">
                                Password
                            </Form.Label>
                            <Col sm="8">
                                <Form.Control type="password" name="password" placeholder={password} onChange={handlePasswordChange} />
                            </Col>
                        </Form.Group>
                        <Form.Group as={Row} className="mb-3" controlId="formPlaintextPassword">
                            <Form.Label column sm="4">
                                Confirm Password
                            </Form.Label>
                            <Col sm="8">
                                <Form.Control type="password" name="confirmpassword" placeholder={confirmpassword} onChange={handleConfirmPasswordChange} />
                            </Col>
                        </Form.Group>

                        <Form.Group as={Row} className="mb-3" controlId="formButtonSignUp">
                            <Col sm="12">
                                <Button variant="outline-secondary" size="sm" onClick={SignUp}>Sign up</Button>
                                <br />
                                <button type="button" class="btn btn-link" onClick={resetToSignInView}>Already a member? Sign In</button>
                            </Col>
                        </Form.Group>
                    </Form>
                </Container>
            </Layout>
        )
    }
    else {
        if (registeredPendingVerification) {
            return (
                <Layout>
                    <Container className="text-center" fluid="md" id="loginContainer">
                        <Form id="loginForm">
                            <Form.Group as={Row} className="mb-3" controlId="formInputVerifyCode">
                                <Form.Label column sm="4">
                                    Code
                                </Form.Label>
                                <Col sm="8">
                                    <Form.Control type="text" placeholder={code} onChange={handleCodeChange} />
                                </Col>
                            </Form.Group>

                            <Form.Group as={Row} className="mb-3" controlId="formButtonVerifyCode">
                                <Col sm="12">
                                    <Button variant="outline-secondary" size="sm" onClick={VerifyCode}>Verify</Button>
                                </Col>
                            </Form.Group>
                        </Form>
                    </Container>
                </Layout>
            )
        }
        else if (registered) {
            return (
                <Layout>
                    <Container className="text-center" fluid="md" id="loginContainer">
                        <Form id="loginForm">
                            <Form.Group as={Row} className="mb-3" controlId="formPlaintextEmailSignUp">
                                <Form.Label column sm="4">
                                    Email
                                </Form.Label>
                                <Col sm="8">
                                    <Form.Control type="text" placeholder={email} onChange={handleEmailChange} />
                                </Col>
                            </Form.Group>

                            <Form.Group as={Row} className="mb-3" controlId="formPlaintextPasswordSignUp">
                                <Form.Label column sm="4">
                                    Password
                                </Form.Label>
                                <Col sm="8">
                                    <Form.Control type="password" placeholder={password} onChange={handlePasswordChange} />
                                </Col>
                            </Form.Group>
                            <Form.Group as={Row} className="mb-3" controlId="formButtonSignUp">
                                <Col sm="12">
                                    <Button variant="outline-secondary" size="sm" onClick={SignIn}>Sign In</Button>
                                    <br />
                                    <button type="button" class="btn btn-link" onClick={resetToSignUpView}>New User? Sign Up</button>
                                </Col>
                            </Form.Group>
                        </Form>
                    </Container>
                </Layout>

            )
        }
    }
}
