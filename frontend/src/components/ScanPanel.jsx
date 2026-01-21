
import React, { useEffect, useState } from 'react';
import { UploadZone } from './UploadZone';
import { ResultCard } from './ResultCard';
import axios from 'axios';
import { Scan, History } from 'lucide-react';

export function ScanPanel() {
    const [history, setHistory] = useState([]);
    const [lastResult, setLastResult] = useState(null);

    const fetchHistory = async () => {
        try {
            const res = await axios.get('http://localhost:8000/history');
            setHistory(res.data);
        } catch (err) {
            console.error(err);
        }
    };

    useEffect(() => {
        fetchHistory();
        const interval = setInterval(fetchHistory, 5000); // Poll for updates
        return () => clearInterval(interval);
    }, []);

    const handleResult = (res) => {
        setLastResult(res);
        fetchHistory();
    };

    return (
        <div className="flex h-full flex-col rounded-2xl border border-white/10 bg-black/40 p-6 backdrop-blur-md">
            <div className="mb-6 flex items-center gap-3 border-b border-white/10 pb-4">
                <Scan className="text-blue-400" />
                <h2 className="text-xl font-bold text-white">Copyright Detection</h2>
            </div>

            <div className="mb-8">
                <UploadZone
                    title="Scan for Copies"
                    endpoint="/query"
                    onResult={handleResult}
                    allowFolder={true}
                />
                <ResultCard result={lastResult} />
            </div>

            <div className="flex-1 overflow-hidden">
                <div className="mb-3 flex items-center justify-between">
                    <h3 className="text-sm font-semibold uppercase tracking-wider text-gray-500">
                        Scan History
                    </h3>
                    <History className="h-4 w-4 text-gray-600" />
                </div>

                <div className="h-full overflow-y-auto pr-2 scrollbar-thin scrollbar-thumb-white/10">
                    <div className="space-y-2">
                        {history.length === 0 ? (
                            <div className="py-8 text-center text-sm text-gray-600">No scans yet</div>
                        ) : (
                            history.map((item, i) => (
                                <div
                                    key={i}
                                    className={`flex items-center justify-between rounded-lg p-3 border-l-2 ${item.match ? 'bg-red-500/10 border-red-500' : 'bg-green-500/10 border-green-500'}`}
                                >
                                    <div className="overflow-hidden">
                                        <div className="truncate text-sm font-medium text-white">{item.query}</div>
                                        <div className="text-xs text-gray-500">{item.timestamp}</div>
                                    </div>
                                    <div className={`text-xs font-bold px-2 py-1 rounded ${item.match ? 'bg-red-500/20 text-red-400' : 'bg-green-500/20 text-green-400'}`}>
                                        {item.match ? `${(item.confidence * 100).toFixed(0)}% Match` : 'Safe'}
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
