import type { ColumnDef } from '@tanstack/react-table'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { DataTable, type DataTableProps } from '@/components/ui/data-table'
import { DataTableColumnHeader } from '@/components/ui/data-table-column-header'
import { DataTableViewOptions } from '@/components/ui/data-table-view-options'

type Row = { id: string; name: string }

const rows: Row[] = Array.from({ length: 25 }, (_, index) => ({ id: `r${index}`, name: `row-${index}` }))

const columns: ColumnDef<Row, unknown>[] = [
  { accessorKey: 'id', header: 'ID' },
  {
    accessorKey: 'name',
    header: ({ column }) => <DataTableColumnHeader column={column} title="Name" />
  }
]

describe('DataTable', () => {
  it('settles after a sort instead of re-rendering in a loop', async () => {
    const user = userEvent.setup()
    let renders = 0
    function TrackedDataTable(props: DataTableProps<Row, unknown>) {
      renders += 1
      return <DataTable {...props} />
    }
    render(<TrackedDataTable columns={columns} data={rows} enablePagination />)

    await user.click(screen.getByRole('button', { name: /Name/i }))
    await user.click(await screen.findByRole('menuitem', { name: 'Asc' }))

    const settledRenders = renders
    await new Promise((resolve) => setTimeout(resolve, 100))
    expect(renders).toBe(settledRenders)
    expect(renders).toBeLessThan(20)
  })

  it('settles after a page change instead of re-rendering in a loop', async () => {
    const user = userEvent.setup()
    render(<DataTable columns={columns} data={rows} enablePagination />)

    await user.click(screen.getByRole('button', { name: /Go to next page/ }))
    expect(screen.getByText('row-10')).toBeInTheDocument()
    expect(screen.queryByText('row-0')).not.toBeInTheDocument()
  })

  it('settles while typing a filter instead of re-rendering in a loop', async () => {
    const user = userEvent.setup()
    let renders = 0
    function TrackedDataTable(props: DataTableProps<Row, unknown>) {
      renders += 1
      return <DataTable {...props} />
    }
    render(<TrackedDataTable columns={columns} data={rows} enablePagination filterColumnId="name" />)

    await user.type(screen.getByLabelText('Filter...'), 'row-1')

    const settledRenders = renders
    await new Promise((resolve) => setTimeout(resolve, 100))
    expect(renders).toBe(settledRenders)
    expect(renders).toBeLessThan(20)
  })

  it('reflects column visibility changes when the Columns menu is reopened', async () => {
    const user = userEvent.setup()
    render(
      <DataTable columns={columns} data={rows} enablePagination>
        {(table) => <DataTableViewOptions table={table} />}
      </DataTable>
    )

    const openColumns = async () => {
      await user.click(screen.getByRole('button', { name: /columns/i }))
      return screen.findByRole('menuitemcheckbox', { name: 'name' })
    }

    const nameItem = await openColumns()
    expect(nameItem).toHaveAttribute('aria-checked', 'true')

    await user.click(nameItem)
    const reopened = await openColumns()
    expect(reopened).toHaveAttribute('aria-checked', 'false')

    await user.click(reopened)
    const reopenedAgain = await openColumns()
    expect(reopenedAgain).toHaveAttribute('aria-checked', 'true')
  })
})
