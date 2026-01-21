
import React, { useEffect, useState } from 'react';
import { UploadZone } from './UploadZone';
import axios from 'axios';
import { Database, FileVideo } from 'lucide-react';
import { motion } from 'framer-motion';

export function IngestPanel() {
    const [inventory, setInventory] = useState([]);
    const [status, setStatus] = useState(null);

    const fetchInventory = async () => {
        try {
            const res = await axios.get('http://localhost:8000/inventory');
            setInventory(res.data.videos);
        } catch (err) {
            console.error(err);
        }
    };

    useEffect(() => {
        fetchInventory();
        const interval = setInterval(fetchInventory, 5000);
        return () => clearInterval(interval);
    }, []);

    const handleResult = (res) => {
        if (res.error) {
            setStatus({ type: 'error', msg: res.error });
        } else {
            setStatus({ type: 'success', msg: `Indexed: ${res.video_id}` });
            fetchInventory();
        }
        setTimeout(() => setStatus(null), 3000);
    };

    return (
        <div className="flex h-full flex-col rounded-2xl border border-white/10 bg-black/40 p-6 backdrop-blur-md">
            <div className="mb-6 flex items-center gap-3 border-b border-white/10 pb-4">
                <Database className="text-purple-400" />
                <h2 className="text-xl font-bold text-white">Fingerprint Database</h2>
            </div>

            <div className="mb-8">
                <UploadZone
                    title="Ingest New Video"
                    endpoint="/ingest"
                    onResult={handleResult}
                    allowFolder={true}
                />
                {status && (
                    <div className={`mt-2 text-center text-sm ${status.type === 'error' ? 'text-red-400' : 'text-green-400'}`}>
                        {status.msg}
                    </div>
                )}
            </div>

            <div className="flex-1 overflow-hidden">
                <h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-gray-500">
                    Indexed Inventory ({inventory.length})
                </h3>
                <div className="h-full overflow-y-auto pr-2 scrollbar-thin scrollbar-thumb-white/10">
                    <div className="space-y-2">
                        {inventory.length === 0 ? (
                            <div className="py-8 text-center text-sm text-gray-600">Database Empty</div>
                        ) : (
                            inventory.map((vid, i) => (
                                <motion.div
                                    key={i}
                                    initial={{ opacity: 0, x: -10 }}
                                    animate={{ opacity: 1, x: 0 }}
                                    transition={{ delay: i * 0.05 }}
                                    className="flex items-center gap-3 rounded-lg bg-white/5 p-3 hover:bg-white/10"
                                >
                                    <FileVideo className="h-4 w-4 text-purple-400" />
                                    <span className="truncate text-sm text-gray-300">{vid}</span>
                                </motion.div>
                            ))
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
