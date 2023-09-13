import { Card, CardContent, CardMedia, Link, Typography } from "@mui/material";
import Grid from "@mui/material/Grid";
import React, { useEffect, useState } from "react";
import { searchYoutube } from "../api/search-api";
import SearchSection from "../components/SearchSection";
import { VideoItem } from "../datatype/SearchData";
import { useQueryParam } from "../hooks/useQueryParam";

const Videos = () => {
  const { query, onSearchQuery } = useQueryParam();
  const [searchTerm, setSearchTerm] = useState<string>(query.get("q") || "");
  const [videos, setVideos] = useState<VideoItem[]>([]);

  const triggerSearch = () => {
    onSearchQuery(searchTerm);
  }

  useEffect(() => {
    const fetchData = async () => {
      if (query.get("q") !== null) {
        const results = await searchYoutube(query.get("q") || "");
        setVideos(results);
      }
    };
    fetchData().catch(console.error);
  }, [query]);

  const getYoutubeUrl = (id: string) => {
    return `https://youtu.be/${id}`;
  };

  return (
    <Grid container style={{padding: '15px'}} spacing={1}>
      <Grid item xs={12}>
        <SearchSection query={searchTerm} onQueryChange={setSearchTerm} onQueryEntered={triggerSearch} showDebugOption={false} />
        <hr />
      </Grid>
      <Grid item xs={12}>
        <Grid container rowSpacing={2} columnSpacing={2}>
        {
          query.get("q") && videos.map((video, index) => (
            <Grid item xs={3} key={index}>
              <Link href={getYoutubeUrl(video.id.videoId)} target="_">
                <Card sx={{height: 230}}>
                  <CardMedia
                    component="img"
                    height={150}
                    image={video.snippet.thumbnails.high.url}
                    alt="thumbnail"
                  />
                  <CardContent>
                    <Typography variant="body2" color="text.secondary">
                      {video.snippet.title}
                    </Typography>
                  </CardContent>
                </Card>
              </Link>
            </Grid>
          ))
        }
        </Grid>
      </Grid>
    </Grid>
  )
}

export default Videos;
