import { Alert, Button, Grid, Paper, TextField } from '@mui/material';
import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { login } from '../api/account-api';
import { User } from '../datatype/User';

const Login = () => {
  const [userForm, setUserForm] = useState("");
  const [password, setPassword] = useState("");
  const [errMessage, setErrMessage] = useState("");
  const navigate = useNavigate();

  const onSubmit = async () => { 
    const [ok, user] = await login(userForm, password);
    if (ok === true) {
      setErrMessage("");
      navigate({pathname: "/search/doc"});
    } else {
      setErrMessage("Username/Password is not correct");
    }
  }

  return (
    <>
    <Link to="/">
      <img 
        src={"/streamwork.png"}
        alt="logo" 
        style={{display: "block", width: "180px", marginLeft: "22px", marginTop: "15px"}} />
    </Link>
    <Grid container spacing={3} style={{marginTop: "10%"}}>
        <Grid item xs={12}>
          <Grid container direction="row">
            <Grid item xs={3}></Grid>
            <Grid item xs={6}>
                <h1>Log In</h1>
                {
                  errMessage.length > 0 && <Alert severity="error">{errMessage}</Alert>
                }
                <TextField fullWidth id="username"
                  placeholder="Enter username"
                  value={userForm}
                  name="username"
                  onChange={(e) => setUserForm(e.target.value)}/>
                <TextField fullWidth id="password"
                  placeholder="Enter password"
                  value={password}
                  name="password"
                  type="password"
                  onChange={(e) => setPassword(e.target.value)}/>
            </Grid>
          </Grid>
        </Grid>
        <Grid item xs={12}>
          <Grid container direction="row" alignItems="center">
            <Grid item xs={4.5}/>
            <Grid item xs={3}>
              <Button variant="contained" color="secondary" size="large"  fullWidth
                onClick={() => onSubmit()}>
                  Submit
              </Button>
            </Grid>
            <Grid item xs={4.5}/>
          </Grid>
        </Grid>
    </Grid>
    </>
  )
}

export default Login;