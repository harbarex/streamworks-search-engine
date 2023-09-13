import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Documents from './pages/Documents';
import Pdfs from './pages/Pdfs';
import Videos from './pages/Videos';
import { ThemeProvider } from '@mui/material';
import { theme } from './theme';
import Welcome from './pages/Welcome';
import Login from './pages/Login';
import Register from './pages/Register';
import Settings from './pages/Settings';
import Trending from './pages/Trending';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);

root.render(
  <ThemeProvider theme={theme}>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Welcome />}></Route>
        <Route path="/login" element={<Login />}></Route>
        <Route path="/register" element={<Register />}></Route>
        {/* <Route path="/admin" element={<App />}>
          <Route path="/admin/settings" element={<Settings />} />
        </Route> */}
        <Route path="/" element={<App />}>
          <Route path="/admin/settings" element={<Settings />} />
          <Route path="/search" element={<Documents />} />
          <Route path="/search/doc" element={<Documents />} />
          <Route path="/search/pdf" element={<Pdfs />} />
          <Route path="/search/video" element={<Videos />} />
          <Route path="/search/trending" element={<Trending />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </ThemeProvider>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
