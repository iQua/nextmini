import { Inter, JetBrains_Mono } from 'next/font/google';
import { GeistMono } from "geist/font/mono";

export const inter = Inter({
  subsets: ['latin'],
  display: 'swap',
})

export const jetbrains_mono = JetBrains_Mono({
  subsets: ['latin'],
  display: 'swap',
})

export default function Nextra({ Component, pageProps }) {
  return (
    <>
      <style jsx global>{`
        html {
          font-family: ${inter.style.fontFamily};
        }
        code {
          font-family: ${GeistMono.style.fontFamily};
        }
        }
      `}</style>
      <Component {...pageProps} />
    </>
  );
}
