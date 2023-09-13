import React, { useState } from "react";
import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
import Divider from '@mui/material/Divider';
import ListItem from '@mui/material/ListItem';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import { adminItems, loggedOutItems, searchItems } from "./consts/navbarItems";
import { useLocation } from "react-router";
import LogoutIcon from '@mui/icons-material/Logout';
import { Link } from "react-router-dom";
import { useQueryParam } from "../../hooks/useQueryParam";

const drawerWidth = 240;

const Navbar = () => {
  const { query } = useQueryParam();
  const { pathname } = useLocation();
  const [username, setUsername] = useState(localStorage.getItem("username"));
  const [isAdmin, setIsAdmin] = useState(localStorage.getItem("isAdmin") === "true");
  
  const logout = () => {
    setUsername("");
    setIsAdmin(false);
    localStorage.removeItem("username");
    localStorage.removeItem("isAdmin");
    localStorage.removeItem("accessToken");
  }

  return (
    <Drawer
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          '& .MuiDrawer-paper': {
            width: drawerWidth,
            boxSizing: 'border-box',
          },
        }}
        variant="permanent"
        anchor="left"
      >
        <Link to="/">
          <img 
            src={"/streamwork.png"}
            alt="logo" 
            style={{display: "block", marginLeft: "auto", marginRight: "auto", marginTop: "15px", width: "80%"}} />
        </Link>
        <List>
          <ListItem 
            button 
            key="search"
            disabled
          >
            <ListItemText primary={"Search"} />
          </ListItem>
          {searchItems.map((item) => (
            <Link 
              to={{pathname: item.route, search: item.route !== "/search/trending" && query.get("q") ? `?q=${query.get("q")}` : ""}} 
              style={{color: "inherit", textDecoration: "none"}}
              key={item.id}
            >
              <ListItem 
                button 
                key={item.id}
                selected={pathname === item.route}
              >
                <ListItemIcon>
                  {item.icon}
                </ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItem>
            </Link>
          ))}
        </List>
        <Divider />
        <List>
          <ListItem 
            button 
            key="account"
            disabled
          >
            <ListItemText primary={`Account${username && username.length > 0 ? ": " + username: ""}`} />
          </ListItem>
          {
            username?.length === 0 ? (
              loggedOutItems.map((item) => (
                <Link 
                  to={{pathname: item.route}} 
                  style={{color: "inherit", textDecoration: "none"}}
                  key={item.id}
                >
                  <ListItem 
                    button 
                    key={item.id}
                  >
                    <ListItemIcon>
                      {item.icon}
                    </ListItemIcon>
                    <ListItemText primary={item.label} />
                  </ListItem>
                </Link>
              ))
            ) : (
              <>
                {isAdmin && adminItems.map((item) => (
                    <Link 
                      to={{pathname: item.route}} 
                      style={{color: "inherit", textDecoration: "none"}}
                      key={item.id}
                    >
                      <ListItem 
                        button 
                        key={item.id}
                      >
                        <ListItemIcon>
                          {item.icon}
                        </ListItemIcon>
                        <ListItemText primary={item.label} />
                      </ListItem>
                    </Link>
                  )) 
                }
                <ListItem 
                  button 
                  key="logout"
                  onClick={logout}
                >
                  <ListItemIcon>
                    <LogoutIcon />
                  </ListItemIcon>
                  <ListItemText primary={"Log out"} />
                </ListItem>
              </>
            )
          }
          
        </List>
      </Drawer>
  )
}

export default Navbar;