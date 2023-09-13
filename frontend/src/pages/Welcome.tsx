import { Button, Grid, MenuItem, Select, SelectChangeEvent, TextField } from "@mui/material";
import React, { useState } from "react";
import { useNavigate } from "react-router";

const Welcome = () => {
  const navigate = useNavigate();
  const [searchTerm, setSearchTerm] = useState("");
  const [searchType, setSearchType] = useState("doc");

  const getSearchUrl = () => {
    return `/search/${searchType}?q=${searchTerm}`;
  }

  const onSearch = () => {
    if (searchTerm.trim().length > 0) {
      navigate(getSearchUrl());
    }
  }

  const onEnter = (e: any) => {
    if (e.key === "Enter") {
      onSearch();
    }
  }

  const handleTypeChange = (event: SelectChangeEvent) => {
    setSearchType(event.target.value as string);
  };

  return (
    <Grid container spacing={3} style={{marginTop: "10%"}}>
      <Grid item xs={12}>
        <img src={"/streamwork.png"} alt="logo" style={{display: "block", marginLeft: "auto", marginRight: "auto"}} />
      </Grid>
      <Grid item xs={12}>
        <Grid container direction="row">
          <Grid item xs={2} />
          <Grid item xs={6}>
          <TextField fullWidth id="searchbox"
            placeholder="What do you want to search today?"
            value={searchTerm}
            name="q"
            onChange={(e) => setSearchTerm(e.target.value)}
            onKeyDown={onEnter} />
          </Grid>
          <Grid item xs={2}>
            <Select
              id="demo-simple-select"
              value={searchType}
              onChange={handleTypeChange}
              fullWidth
            >
              <MenuItem value={"doc"}>Page</MenuItem>
              <MenuItem value={"video"}>Video</MenuItem>
            </Select>
          </Grid>
          <Grid item xs={2} />
        </Grid>
      </Grid>
      <Grid item xs={12}>
        <Grid container direction="row" alignItems="center">
          <Grid item xs={4.5}/>
          <Grid item xs={3}>
          <Button variant="contained" color="secondary" size="large"  fullWidth
            onClick={() => onSearch()}>
              Search
          </Button>
          </Grid>
          <Grid item xs={4.5}/>
        </Grid>
        
      </Grid>
    </Grid>
  )
}

export default Welcome;
