import AxeBuilder from '@axe-core/playwright'
import { expect, test } from '../fixtures/base'

const accents = [undefined, 'blue', 'cyan', 'violet', 'green', 'amber', 'pink'] as const
const themes = ['light', 'dark'] as const

test('request logs keeps text, controls, and live status badges AA-compliant across themes and accents', async ({
  page
}) => {
  await page.route('**/api/logs/requests', (route) =>
    route.fulfill({
      json: {
        items: [
          {
            requestId: '00000000-0000-4000-8000-000000000001',
            outcome: 'active',
            createdAt: '2026-08-04T12:00:00Z',
            terminalAt: null,
            route: 'reserve',
            model: 'Qwen3',
            provider: 'reserve-a',
            engine: 'skippy',
            statusCode: null,
            source: 'active'
          },
          {
            requestId: '00000000-0000-4000-8000-000000000002',
            outcome: 'completed',
            createdAt: '2026-08-04T12:01:00Z',
            terminalAt: '2026-08-04T12:01:01Z',
            route: 'reserve',
            model: 'Qwen3',
            provider: 'reserve-a',
            engine: 'skippy',
            statusCode: 200,
            source: 'durable'
          },
          {
            requestId: '00000000-0000-4000-8000-000000000003',
            outcome: 'cancelled',
            createdAt: '2026-08-04T12:02:00Z',
            terminalAt: '2026-08-04T12:02:01Z',
            route: 'reserve',
            model: 'Qwen3',
            provider: 'reserve-a',
            engine: 'skippy',
            statusCode: null,
            source: 'durable'
          },
          {
            requestId: '00000000-0000-4000-8000-000000000004',
            outcome: 'failed',
            createdAt: '2026-08-04T12:03:00Z',
            terminalAt: '2026-08-04T12:03:01Z',
            route: 'reserve',
            model: 'Qwen3',
            provider: 'reserve-a',
            engine: 'skippy',
            statusCode: 500,
            source: 'durable'
          }
        ],
        nextCursor: null
      }
    })
  )
  await page.goto('/logs')
  await expect(page.getByRole('button', { name: 'Scoped cleanup' })).toBeVisible()
  await expect(page.getByText('Reconnecting', { exact: true })).toBeVisible()
  const root = page.locator('html')
  await expect(root).toHaveAttribute('data-theme-preference')

  for (const theme of themes) {
    for (const accent of accents) {
      await root.evaluate(
        (element, preference) => {
          element.dataset.theme = preference.theme
          if (preference.accent === undefined) {
            delete element.dataset.accent
          } else {
            element.dataset.accent = preference.accent
          }
        },
        { theme, accent }
      )
      await expect(root).toHaveAttribute('data-theme', theme)
      await expect(root).toHaveCSS('color-scheme', theme)
      if (accent === undefined) {
        await expect(root).not.toHaveAttribute('data-accent')
      } else {
        await expect(root).toHaveAttribute('data-accent', accent)
      }
      await expect
        .poll(() =>
          root.evaluate(() =>
            document
              .getAnimations({ subtree: true })
              .filter((animation) => animation instanceof CSSTransition)
              .every((transition) => transition.playState !== 'running')
          )
        )
        .toBe(true)

      const results = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa']).analyze()
      expect(results.violations).toEqual([])
    }
  }
})
