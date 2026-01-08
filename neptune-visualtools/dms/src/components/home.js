import Layout, { siteTitle } from './layout'
import {
  Navbar, Nav, NavDropdown, Form, FormControl, Button, Card, Container
  , Row, Col, ListGroup, Dropdown, FloatingLabel, Link, Image
} from 'react-bootstrap';
import 'bootstrap/dist/css/bootstrap.min.css';
import React, { useState, useEffect, useHistory, useLocation } from 'react';
import { v4 as uuidv4 } from 'uuid';
import { BrowserRouter as Router, Switch, Route, Redirect } from "react-router-dom";
import { sectionFooterSecondaryContent } from '@aws-amplify/ui';
import Connections from './connections'

function Home() {
  return (
    <Layout home>
      
    </Layout>
  );
}

export default Home;
