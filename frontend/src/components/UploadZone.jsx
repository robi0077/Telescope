
import React, { useState } from 'react';
import { Upload, FolderPlus, FileVideo } from 'lucide-react';
import { motion } from 'framer-motion';
import { clsx } from 'clsx';
import axios from 'axios';

export function UploadZone({ onResult, endpoint, title, allowFolder = false }) {
    const [isDragging, setIsDragging] = useState(false);
    const [isUploading, setIsUploading] = useState(false);

    // Sanitize endpoint to be safe for HTML IDs (replace / with -)
    const safeId = endpoint.replace(/\//g, '-');


    const handleDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === "dragenter" || e.type === "dragover") {
            setIsDragging(true);
        } else if (e.type === "dragleave") {
            setIsDragging(false);
        }
    };

    const handleDrop = async (e) => {
        e.preventDefault();
        e.stopPropagation();
        setIsDragging(false);

        // For simplicity in this demo, we handle the first file even if folder dropped
        // Real folder handling requires recursive reading of FileSystemEntry
        if (e.dataTransfer.files && e.dataTransfer.files[0]) {
            uploadFile(e.dataTransfer.files[0]);
        }
    };

    const handleFileSelect = (e) => {
        if (e.target.files && e.target.files[0]) {
            uploadFile(e.target.files[0]);
        }
    };

    const uploadFile = async (file) => {
        setIsUploading(true);
        const formData = new FormData();
        formData.append("file", file);

        try {
            const response = await axios.post(`http://localhost:8000${endpoint}`, formData, {
                headers: {
                    'Content-Type': 'multipart/form-data',
                },
            });

            onResult(response.data);
        } catch (error) {
            console.error("Upload failed", error);
            onResult({ error: "Upload failed. Is backend running?" });
        } finally {
            setIsUploading(false);
        }
    };

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className={clsx(
                "relative rounded-xl border-2 border-dashed p-8 text-center transition-all duration-300",
                isDragging
                    ? "border-blue-500 bg-blue-500/10"
                    : "border-white/10 bg-black/20 hover:border-white/30"
            )}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
        >
            <input
                type="file"
                className="hidden"
                style={{ display: 'none' }}
                accept="video/*,.mkv,.mp4,.mov,.avi,.flv,.wmv,.webm,.ts,.mts,.m2ts"
                id={`file-upload-${safeId}`}
                onChange={handleFileSelect}
            />

            {allowFolder && (
                <input
                    type="file"
                    className="hidden"
                    style={{ display: 'none' }}
                    id={`folder-upload-${safeId}`}
                    webkitdirectory=""
                    directory=""
                    onChange={handleFileSelect}
                />
            )}

            <div className="flex flex-col items-center gap-3">
                {isUploading ? (
                    <motion.div
                        animate={{ rotate: 360 }}
                        transition={{ repeat: Infinity, duration: 1, ease: "linear" }}
                    >
                        <div className="h-10 w-10 rounded-full border-4 border-blue-500 border-t-transparent" />
                    </motion.div>
                ) : (
                    <div className="rounded-full bg-white/5 p-3">
                        {allowFolder ? <FolderPlus className="h-6 w-6 text-purple-400" /> : <Upload className="h-6 w-6 text-blue-400" />}
                    </div>
                )}

                <div>
                    <h3 className="text-md font-medium text-white">
                        {isUploading ? "Processing..." : title || "Upload Video"}
                    </h3>
                    <div className="mt-2 flex justify-center gap-4 text-xs text-gray-400">
                        <label htmlFor={`file-upload-${safeId}`} className="cursor-pointer hover:text-white transition-colors">
                            [ Select File ]
                        </label>
                        {allowFolder && (
                            <label htmlFor={`folder-upload-${safeId}`} className="cursor-pointer hover:text-white transition-colors">
                                [ Select Folder ]
                            </label>
                        )}
                    </div>
                </div>
            </div>
        </motion.div>
    );
}
