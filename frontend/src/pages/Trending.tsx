import { Avatar, Chip, Grid, Link, Paper, Skeleton, Typography } from "@mui/material";
import React, { useEffect, useState } from "react";
import { getKeywordCount } from "../api/keyword-api";
import { KeywordCount } from "../datatype/KeywordCount";

const Trending = () => {
  const [kwCount, setKwCount] = useState<KeywordCount[]>([]);
  const [loading, setLoading] = useState(false);

  const delay = (ms: number) => {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  useEffect(() => {
    setLoading(true);
    const fetchCount = async () => {
      const count = await getKeywordCount();
      setKwCount(count);
      await delay(1000);
      setLoading(false);
    };
    fetchCount().catch(console.error);
  }, []);

  return (
    <Grid container style={{padding: '15px', marginTop: "10%"}} spacing={1}>
      <Grid item xs={2} />
      <Grid item xs={8}>
        <Typography variant="h4">People are searching for these keywords</Typography>
        <br />
        {
          loading && <>
          <Skeleton animation="wave" style={{width: "100%"}} />
          <Skeleton animation="wave" style={{width: "100%"}} />
          <Skeleton animation="wave" style={{width: "100%"}} />
          <Skeleton animation="wave" style={{width: "100%"}} />
          <Skeleton animation="wave" style={{width: "100%"}} />
          <Skeleton animation="wave" style={{width: "100%"}} />
          </>
        }
        {
          !loading && <Paper elevation={4} style={{padding: "20px"}}>
            {
              kwCount.map((count) => 
              <Link href={`/search/doc?q=${count.keyword}`}>
                <Chip id={count.keyword} avatar={<Avatar>{count.count}</Avatar>} label={count.keyword} />
              </Link>)
            }
          </Paper>
        }
        
      </Grid>
    </Grid>
  )
}

export default Trending;