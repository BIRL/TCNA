
import { useCallback, useEffect } from "react";

// Default cache duration: 1 day in milliseconds
const DEFAULT_CACHE_DURATION = 24 * 60 * 60 * 1000;

// Interface for cached data with expiration
interface CachedData<T> {
  data: T;
  expiresAt: number;
}

// Generate a cache key based on parameters
const generateCacheKey = (params: Record<string, any>): string => {
  try {
    return JSON.stringify(params);
  } catch (error) {
    console.error("Error generating cache key:", error);
    return "";
  }
};

// Reusable cache hook
export const useCache = <T>(cacheDuration: number = DEFAULT_CACHE_DURATION) => {
  // Get cached data
  const getCachedData = useCallback((key: string): T | null => {
    try {
      const cached = localStorage.getItem(key);
      if (!cached) return null;

      const { data, expiresAt }: CachedData<T> = JSON.parse(cached);
      if (Date.now() > expiresAt) {
        localStorage.removeItem(key); // Remove expired cache
        return null;
      }
      return data;
    } catch (error) {
      console.error("Error reading cache:", error);
      return null;
    }
  }, []);

  // Set cached data
  const setCachedData = useCallback((key: string, data: T, duration: number = cacheDuration) => {
    try {
      const cachedData: CachedData<T> = {
        data,
        expiresAt: Date.now() + duration,
      };
      localStorage.setItem(key, JSON.stringify(cachedData));
    } catch (error) {
      console.error("Error writing to cache:", error);
    }
  }, [cacheDuration]);

  // Clear specific cache entry
  const clearCacheEntry = useCallback((key: string) => {
    try {
      localStorage.removeItem(key);
    } catch (error) {
      console.error("Error clearing cache entry:", error);
    }
  }, []);

  // Clear all cache entries
  const clearAllCache = useCallback(() => {
    try {
      localStorage.clear();
    } catch (error) {
      console.error("Error clearing all cache:", error);
    }
  }, []);
  

  // Automatically clear expired cache entries periodically
  useEffect(() => {
    const checkAndClearExpired = () => {
      try {
        for (let i = 0; i < localStorage.length; i++) {
          const key = localStorage.key(i);
          if (key) {
            const cached = localStorage.getItem(key);
            if (cached) {
              const { expiresAt }: CachedData<T> = JSON.parse(cached);
              if (Date.now() > expiresAt) {
                localStorage.removeItem(key);
              }
            }
          }
        }
      } catch (error) {
        console.error("Error during periodic cache cleanup:", error);
      }
    };

    // Run cleanup immediately and then every 10 minutes
    checkAndClearExpired();
    const intervalId = setInterval(checkAndClearExpired, 10 * 60 * 1000);

    // Cleanup interval on unmount
    return () => clearInterval(intervalId);
  }, []);

  return {
    getCachedData,
    setCachedData,
    // clearCacheEntr y,
    // clearAllCache,
    generateCacheKey,
  };
};