import React, { useEffect, useState } from "react";
import { FormControlLabel, Grid, Link, Paper, Switch, TextField, Typography } from "@mui/material";
import { checkSpelling } from "../api/keyword-api";

const SearchSection = ({ query, onQueryChange, onQueryEntered, debugMode, onDebugModeChange, showDebugOption }: {
  query: string;
  onQueryEntered: () => void;
  onQueryChange: (term: string) => void;
  showDebugOption: boolean;
  debugMode?: boolean;
  onDebugModeChange?: (e: React.ChangeEvent<HTMLInputElement>) => void;
}) => {
  const [spellcheck, setSpellcheck] = useState(query);
  const [displaySpellcheck, setDisplaySpellcheck] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      const spellCheckResult = await checkSpelling(query);
      setSpellcheck(spellCheckResult);
    };
    fetchData().catch(console.error);
  }, [query]);

  const handleKeydown = (e: any) => {
    if (e.key === "Enter") {
      onQueryEntered();
      setDisplaySpellcheck(true);
    } else {
      setDisplaySpellcheck(false);
    }
  }

  return (
    // <Paper>
      <Grid container spacing={2}>
        <Grid item xs={12}>
          <Grid container direction="row" spacing={2} alignItems="center">
            <Grid item xs={showDebugOption ? 10 : 12}>
              <TextField fullWidth id="searchbox"
                placeholder="Enter your search term here"
                value={query}
                name="q"
                onChange={(e) => onQueryChange(e.target.value)}
                onKeyDown={handleKeydown} />
            </Grid>
            {
              showDebugOption && 
              <Grid item xs={2}>
                <FormControlLabel control={<Switch checked={debugMode} onChange={onDebugModeChange} inputProps={{ 'aria-label': 'controlled' }} />} label="Debug Mode" />
              </Grid>
            }
          </Grid>
        </Grid>
       {
         displaySpellcheck && query !== spellcheck &&  <Grid item xs={12}>
           <Typography>
             <b>Do you mean <Link href={`/search/doc?q=${spellcheck}`} style={{textDecoration: "none"}}>{spellcheck}</Link>?
             </b></Typography>
         </Grid>
       }
      </Grid>
    // </Paper>
  )
}

export default SearchSection;
