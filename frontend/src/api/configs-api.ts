const API_HOST = process.env.REACT_APP_ENV === "prod" ? process.env.REACT_APP_API : "";

export const getConfigs = async () => {
  const url = `${API_HOST}/api/configs`;
  const resp = await fetch(url, {
    method: "GET",
    headers: new Headers({
      'Authorization': 'Bearer ' + localStorage.getItem("accessToken")
    })
  });
  const configs = await resp.json();
  return configs as {[name: string]: number};
}

export const updateConf = async (name: string, value: number) => {
  const url = `${API_HOST}/api/configs/${name}`;
  await fetch(url, {
    method: "PUT",
    headers: new Headers({
      'Authorization': 'Bearer ' + localStorage.getItem("accessToken"),
      'Content-Type': 'application/json',
    }),
    body: "" + value,
  });
  return true;
}
