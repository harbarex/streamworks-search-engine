import Grid from '@mui/material/Grid';
import React from 'react';
import { Outlet } from 'react-router';
import './App.css';
import Navbar from './components/navbar/Navbar';

const App = () => {
  return (
    <Grid container direction="row">
      <Grid item xs={2}>
        <Navbar />
      </Grid>
      <Grid item xs={10}>
        <Outlet />
      </Grid>      
    </Grid>
  );
}

export default App;
