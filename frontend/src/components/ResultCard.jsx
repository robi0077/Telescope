
import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ShieldAlert, ShieldCheck, Activity } from 'lucide-react';

export function ResultCard({ result }) {
    if (!result) return null;

    const isMatch = result.is_match;
    const isError = result.error;

    return (
        <AnimatePresence>
            <motion.div
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0 }}
                className="mt-8 overflow-hidden rounded-2xl bg-glass border border-glassBorder p-6 shadow-2xl backdrop-blur-md"
            >
                <div className="flex items-start gap-6">
                    <div className={`rounded-xl p-4 ${isMatch ? "bg-red-500/20 text-red-400" : "bg-green-500/20 text-green-400"}`}>
                        {isError ? <Activity className="h-8 w-8 text-yellow-500" /> :
                            isMatch ? <ShieldAlert className="h-8 w-8" /> : <ShieldCheck className="h-8 w-8" />}
                    </div>

                    <div className="flex-1">
                        <h3 className="text-xl font-bold text-white">
                            {isError ? "System Error" :
                                isMatch ? "Copyright Infringement Detected" : "No Content Matches Found"}
                        </h3>

                        {isError ? (
                            <p className="mt-1 text-yellow-400 font-mono text-sm">{result.error}</p>
                        ) : (
                            <div className="mt-4 space-y-4">
                                <div className="flex items-center justify-between border-b border-white/10 pb-4">
                                    <span className="text-gray-400">Confidence Score</span>
                                    <span className={`text-2xl font-mono ${isMatch ? "text-red-400" : "text-green-400"}`}>
                                        {(result.confidence * 100).toFixed(1)}%
                                    </span>
                                </div>

                                {isMatch && (
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="rounded-lg bg-black/20 p-3">
                                            <div className="text-xs uppercase text-gray-500">Source ID</div>
                                            <div className="font-mono text-sm text-white">{result.video_id}</div>
                                        </div>
                                        <div className="rounded-lg bg-black/20 p-3">
                                            <div className="text-xs uppercase text-gray-500">Alignment Offset</div>
                                            <div className="font-mono text-sm text-white">{result.alignment?.offset}s</div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            </motion.div>
        </AnimatePresence>
    );
}
