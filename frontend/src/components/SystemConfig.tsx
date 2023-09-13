import { Alert, Button, CircularProgress, Grid, Snackbar, TextField, Typography } from "@mui/material";
import React, { useEffect, useState } from "react";
import { getConfigs, updateConf } from "../api/configs-api";

const SystemConfig = () => {
  const [configs, setConfigs] = useState<{[name: string]: number}>();
  const [loading, setLoading] = useState(false);
  const [showAlert, setShowAlert] = useState(false);

  useEffect(() => {
    setLoading(true);
    const fetchConfigs = async () => {
      const fetchedConfigs = await getConfigs();
      console.log("***", fetchedConfigs);
      setConfigs(fetchedConfigs);
      setLoading(false);
    };
    fetchConfigs().catch(console.error);
  }, []);

  const handleConfigChange = (e: any, name: string) => {
    setConfigs((prev) => {
      return {
        ...prev, 
        [name]: +e.target.value
      }
    })
  }

  const updateConfig = async (name: string) => {
    if (configs) {
      await updateConf(name, configs[name]);
      setShowAlert(true);
    }
  }

  return (
    <Grid container spacing={2}> 
      {
        <Snackbar open={showAlert} autoHideDuration={2000} onClose={() => setShowAlert(false)}>
          <Alert onClose={() => setShowAlert(false)} severity="success" sx={{ width: '100%' }}>
            Update successfully!
          </Alert>
        </Snackbar>      
      }
      {
        loading && <Grid item xs={12}>
          <CircularProgress />
        </Grid>
      }
      <>
      {
        !loading && <Grid item xs={12}>
          <Grid container alignItems="center">
            <Grid item xs={2}>
              <Typography><b>Config Name</b></Typography>
            </Grid>
            <Grid item xs={8}>
              <Typography><b>Config Value</b></Typography>
            </Grid>
          </Grid>
        </Grid>
      }
      {
        !loading && configs && Object.keys(configs).map((name) => {
          return <Grid item xs={12}>
            <Grid container alignItems="center" spacing={2}>
              <Grid item xs={2}>
                <Typography>{name}</Typography>
              </Grid>
              <Grid item xs={8}>
                <TextField value={configs[name]} onChange={(e) => handleConfigChange(e, name)} fullWidth />
              </Grid>
              <Grid item xs={2}>
                <Button onClick={() => updateConfig(name)} variant="contained">
                  Update
                </Button>
              </Grid>
            </Grid>
          </Grid>
        })
      }
      </>
    </Grid>
  );
}

export default SystemConfig;