
import React from 'react';
import { IngestPanel } from './components/IngestPanel';
import { ScanPanel } from './components/ScanPanel';
import { ScanEye } from 'lucide-react';

function App() {
  return (
    <div className="min-h-screen w-full bg-[#050505] text-white overflow-hidden flex flex-col">
      <header className="flex-none p-6 border-b border-white/5 bg-black/20 backdrop-blur-sm">
        <div className="mx-auto max-w-7xl flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="rounded-xl bg-blue-600/20 p-2 text-blue-400">
              <ScanEye className="h-6 w-6" />
            </div>
            <div>
              <h1 className="text-2xl font-black tracking-tighter bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
                TELESCOPE
              </h1>
            </div>
          </div>
          <div className="text-xs font-mono text-gray-500">
            v4.3 • System Online
          </div>
        </div>
      </header>

      <main className="flex-1 overflow-hidden flex items-center justify-center p-6">
        <div className="mx-auto w-full max-w-7xl h-[80vh] grid grid-cols-2 gap-8">
          <div className="h-full overflow-hidden flex flex-col justify-center">
            <IngestPanel />
          </div>
          <div className="h-full overflow-hidden flex flex-col justify-center">
            <ScanPanel />
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
