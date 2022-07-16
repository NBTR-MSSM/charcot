import { createContext } from 'react'
import Filter from './Filter'

export const AppContext = createContext({
  isAuthenticated: false,
  routeState: {},
  filter: new Filter(),
  dimensionData: [],
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleLogin: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleLogout: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  redirect: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleRouteLoad: () => {}
})
