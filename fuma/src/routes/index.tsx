import { createFileRoute, redirect } from '@tanstack/react-router';

export const Route = createFileRoute('/')({
  loader: () => {
    throw redirect({
      to: '/docs/$',
      params: {
        _splat: '',
      },
    });
  },
  component: Home,
});

function Home() {
  return null;
}
