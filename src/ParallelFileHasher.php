<?php

declare(strict_types=1);

/*
 * This file is part of Ymir command-line tool.
 *
 * (c) Carl Alexander <support@ymirapp.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ymir\Cli;

use Illuminate\Support\Collection;
use Symfony\Component\Process\Process;

class ParallelFileHasher
{
    /**
     * The number of files to hash per process.
     *
     * @var int
     */
    private $chunkSize;
    /**
     * The maximum number of concurrent hash processes.
     *
     * @var int
     */
    private $concurrency;

    /**
     * Constructor.
     */
    public function __construct(int $concurrency = 8, int $chunkSize = 50)
    {
        $this->concurrency = $concurrency;
        $this->chunkSize = $chunkSize;
    }

    /**
     * Hash multiple files in parallel using the specified algorithm.
     *
     * @param array  $files     Array of file paths to hash
     * @param string $algorithm Hash algorithm to use (md5, sha256, etc.)
     *
     * @return Collection Collection with file paths as keys and hashes as values
     */
    public function hashFiles(array $files, string $algorithm = 'md5'): Collection
    {
        if (empty($files)) {
            return collect([]);
        }

        // Chunk files into batches for parallel processing
        $chunks = array_chunk($files, $this->chunkSize, true);
        $results = [];

        // Process chunks in parallel with concurrency limit
        foreach (array_chunk($chunks, $this->concurrency, true) as $batchOfChunks) {
            $processes = [];

            // Start a process for each chunk in this batch
            foreach ($batchOfChunks as $chunkIndex => $chunk) {
                $processes[$chunkIndex] = $this->createHashProcess($chunk, $algorithm);
                $processes[$chunkIndex]->start();
            }

            // Wait for all processes in this batch to complete
            foreach ($processes as $chunkIndex => $process) {
                $process->wait();

                if ($process->isSuccessful()) {
                    $output = $process->getOutput();
                    $chunkResults = json_decode($output, true);

                    if (is_array($chunkResults)) {
                        $results = array_merge($results, $chunkResults);
                    }
                }
            }
        }

        return collect($results);
    }

    /**
     * Hash multiple files with additional metadata in parallel.
     *
     * @param array  $files     Array of file info arrays with 'real_path' and 'relative_path' keys
     * @param string $algorithm Hash algorithm to use (md5, sha256, etc.)
     *
     * @return Collection Collection of file info arrays with added 'hash' key
     */
    public function hashFilesWithMetadata(array $files, string $algorithm = 'md5'): Collection
    {
        if (empty($files)) {
            return collect([]);
        }

        // Extract file paths for hashing
        $filePaths = array_map(function ($file) {
            return $file['real_path'];
        }, $files);

        // Hash files in parallel
        $hashes = $this->hashFiles($filePaths, $algorithm);

        // Merge hashes back into file metadata
        return collect($files)->map(function ($file) use ($hashes) {
            $file['hash'] = $hashes->get($file['real_path'], '');

            return $file;
        });
    }

    /**
     * Create a PHP process that hashes a chunk of files.
     *
     * @param array  $files     Array of file paths to hash
     * @param string $algorithm Hash algorithm to use
     */
    private function createHashProcess(array $files, string $algorithm): Process
    {
        // Escape file paths for JSON
        $filesJson = json_encode(array_values($files), JSON_UNESCAPED_SLASHES);
        $algorithmEscaped = escapeshellarg($algorithm);

        // Create a PHP script that hashes the files and outputs JSON
        $script = <<<PHP
<?php
\$files = json_decode('{$filesJson}', true);
\$algorithm = {$algorithmEscaped};
\$results = [];

foreach (\$files as \$file) {
    if (file_exists(\$file) && is_readable(\$file)) {
        \$results[\$file] = hash_file(\$algorithm, \$file);
    }
}

echo json_encode(\$results);
PHP;

        // Run the script as a separate PHP process
        return new Process([PHP_BINARY, '-r', $script]);
    }
}
