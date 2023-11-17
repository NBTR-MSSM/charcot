import { createContext } from 'react'
import Filter from './Filter'

export const AppContext = createContext({
  isAuthenticated: false,
  otherUserEmail: '',
  isAdmin: false,
  isResultsFound: false,
  email: '',
  filter: new Filter(),
  dimensionData: {
    dimensions: []
  },
  transactionData: {
    requests: 0
  },
  sessionInfo: undefined,
  transactionItem: undefined,
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  redirectToPrevious: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleCategorySelect: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleCategoryUnselect: () => {},
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
  handleClearFilter: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleSetOtherUserEmail: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleTransactionUpdate: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleSetTransactionItem: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleSetSessionInfo: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  handleChartDataStateUpdate: () => {}
})
