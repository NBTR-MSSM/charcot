import { createContext } from 'react'
import Filter from './Filter'

export const AppContext = createContext({
  isAuthenticated: false,
  email: '',
  filter: new Filter(),
  dimensionData: {
    dimensions: []
  },
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  redirectToPrevious: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleLogin: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleLogout: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  redirect: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  currentPage: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  pushToHistory: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleClearFilter: () => {}
})
