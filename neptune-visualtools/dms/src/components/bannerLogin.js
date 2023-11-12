// import Image from 'next/image'
import {
    Navbar, Nav, NavDropdown, Form, FormControl, Button, Card, Container, Row, Col, ListGroup, Dropdown, FloatingLabel, Image
} from 'react-bootstrap';
import React, { useState, useEffect } from 'react';
import { Auth } from 'aws-amplify';
import {
    BrowserRouter as Router,
    useHistory,
    useLocation
} from "react-router-dom";

export default function BannerLogin() {

    const [user, setUser] = useState('');
    const history = useHistory();
    useEffect(() => {
        Auth.currentAuthenticatedUser()
            .then(
                newuser => {
                    console.log(newuser.attributes.email);
                    setUser(user => {
                        return newuser.attributes.email
                    });
                }
            ).catch(err => console.log(err));
    }, []);

    async function SignIn() {
        history.push('/login');
    }

    async function SignOut() {
        try {
            await Auth.signOut({ global: true });
            history.push('/login');
        } catch (error) {
            console.log('error signing out: ', error);
        }
    }

    if (user === '') {
        return <Button variant="outline-primary" onClick={SignIn}>sign in</Button>;
    } else {
        return <div className="signedProfile">
            <Nav.Item>
                {/* <label className="bannerText"> signed as: {user}</label> */}
                <NavDropdown title={user} id="nav-dropdown">
                    <NavDropdown.Item eventKey="4.1">Your profile</NavDropdown.Item>
                    <NavDropdown.Item eventKey="4.1">Your settings</NavDropdown.Item>
                    <NavDropdown.Divider />
                    <NavDropdown.Item eventKey="4.4"><Button variant="outline-primary" onClick={SignOut}>sign out</Button></NavDropdown.Item>
                </NavDropdown>
            </Nav.Item>

        </div >;
    }
}