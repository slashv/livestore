import { queryDb } from '@livestore/livestore'
import { StoreRegistry, StoreRegistryProvider } from '@livestore/react'
import React, { Suspense } from 'react'
import { createRoot } from 'react-dom/client'

import { events, tables } from './livestore/schema.ts'
import { useAppStore } from './livestore/store.ts'

const storeRegistry = new StoreRegistry()

// Query the clientDocument
const itemsState$ = queryDb(tables.itemsState.get(), { label: 'itemsState' })

class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null }
  static getDerivedStateFromError(error: Error) {
    return { error }
  }
  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error('ErrorBoundary caught:', error, info)
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ color: 'red', padding: 20 }}>
          <h2>Error</h2>
          <pre>{String(this.state.error)}</pre>
          <pre>{this.state.error.stack}</pre>
        </div>
      )
    }
    return this.props.children
  }
}

const AppBody = () => {
  const store = useAppStore()

  const itemsState = store.useQuery(itemsState$)
  const itemCount = Object.keys(itemsState.items).length

  const addItem = () => {
    const id = crypto.randomUUID()
    store.commit(
      events.itemsStateSet({
        items: {
          ...itemsState.items,
          [id]: { value: `Item ${id.slice(0, 8)}`, status: 'pending' },
        },
      }),
    )
  }

  return (
    <div>
      <h1>Repro: clientDocument Changeset Bug</h1>
      <button data-testid="add-item" onClick={addItem}>
        Add Item
      </button>
      <div>
        Count: <span data-testid="item-count">{itemCount}</span>
      </div>
      <div data-testid="livestore-ready">ready</div>
      <ul>
        {Object.entries(itemsState.items).map(([id, item]) => (
          <li key={id}>
            {item.value} - {item.status}
          </li>
        ))}
      </ul>
    </div>
  )
}

const App = () => {
  return (
    <StoreRegistryProvider storeRegistry={storeRegistry}>
      <ErrorBoundary>
        <Suspense fallback={<div>Loading LiveStore...</div>}>
          <AppBody />
        </Suspense>
      </ErrorBoundary>
    </StoreRegistryProvider>
  )
}

createRoot(document.getElementById('root')!).render(<App />)
