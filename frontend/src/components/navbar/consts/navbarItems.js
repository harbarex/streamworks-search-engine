import ArticleIcon from '@mui/icons-material/Article';
// import PictureAsPdfIcon from '@mui/icons-material/PictureAsPdf';
import VideoLibraryIcon from '@mui/icons-material/VideoLibrary';
import LoginIcon from '@mui/icons-material/Login';
import AppRegistrationIcon from '@mui/icons-material/AppRegistration';
import SettingsIcon from '@mui/icons-material/Settings';
import TrendingUpIcon from '@mui/icons-material/TrendingUp';


export const searchItems = [
  {
    id: "trending",
    icon: <TrendingUpIcon />,
    label: "Trending",
    route: "/search/trending"
  },
  {
    id: "doc",
    icon: <ArticleIcon />,
    label: 'Page',
    route: '/search/doc',
  },
  // {
  //   id: "pdf",
  //   icon: <PictureAsPdfIcon />,
  //   label: 'PDF',
  //   route: 'pdf'
  // },
  {
    id: "video",
    icon: <VideoLibraryIcon />,
    label: 'Video',
    route: '/search/video'
  }
]

export const loggedOutItems = [
  {
    id: "login",
    icon: <LoginIcon />,
    label: 'Log in',
    route: "/login"
  },
  {
    id: "register",
    icon: <AppRegistrationIcon />,
    label: "Register",
    route: "/register"
  }
]

export const adminItems = [
  {
    id: "settings",
    icon: <SettingsIcon />,
    label: 'Settings',
    route: "/admin/settings"
  }
]
