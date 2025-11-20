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

use GuzzleHttp\ClientInterface;
use GuzzleHttp\Pool;
use GuzzleHttp\Psr7\Request;
use Illuminate\Support\Enumerable;
use Symfony\Component\Console\Exception\RuntimeException;
use Symfony\Component\Console\Helper\ProgressBar;

class FileUploader
{
    /**
     * The default headers to send with our requests.
     *
     * @var array
     */
    private const DEFAULT_HEADERS = ['Cache-Control' => 'public, max-age=2628000'];

    /**
     * The HTTP client used to upload files.
     *
     * @var ClientInterface
     */
    private $client;

    /**
     * The maximum number of concurrent uploads.
     *
     * @var int
     */
    private $concurrency;

    /**
     * The connection timeout in seconds.
     *
     * @var int
     */
    private $connectTimeout;

    /**
     * The maximum number of retry attempts.
     *
     * @var int
     */
    private $maxRetries;

    /**
     * The request timeout in seconds.
     *
     * @var int
     */
    private $timeout;

    /**
     * Constructor.
     */
    public function __construct(ClientInterface $client, int $concurrency = 15, int $connectTimeout = 10, int $maxRetries = 5, int $timeout = 300)
    {
        $this->client = $client;
        $this->concurrency = $concurrency;
        $this->connectTimeout = $connectTimeout;
        $this->maxRetries = $maxRetries;
        $this->timeout = $timeout;
    }

    /**
     * Sends multiple requests concurrently with retry logic.
     */
    public function batch(string $method, Enumerable $requests, ?ProgressBar $progressBar = null)
    {
        $requestsArray = $requests instanceof \Illuminate\Support\Collection ? $requests->all() : iterator_to_array($requests);
        $totalRequests = count($requestsArray);

        if ($progressBar instanceof ProgressBar) {
            $progressBar->start($totalRequests);
        }

        $this->retryBatch($method, $requestsArray, $progressBar, $this->maxRetries);

        if ($progressBar instanceof ProgressBar) {
            $progressBar->finish();
        }
    }

    /**
     * Upload the given file to the given URL.
     */
    public function uploadFile(string $filePath, string $url, array $headers = [], ?ProgressBar $progressBar = null)
    {
        if (!is_readable($filePath)) {
            throw new RuntimeException(sprintf('Cannot read the "%s" file', $filePath));
        }

        $progressCallback = null;
        $file = fopen($filePath, 'r');

        if (!is_resource($file)) {
            throw new RuntimeException(sprintf('Cannot open the "%s" file', $filePath));
        }

        try {
            if ($progressBar instanceof ProgressBar) {
                $progressBar->start((int) round(filesize($filePath) / 1024));

                $progressCallback = function ($_, $__, $___, $uploaded) use ($progressBar) {
                    $progressBar->setProgress((int) round($uploaded / 1024));
                };
            }

            $this->retry(function () use ($file, $headers, $progressCallback, $url) {
                $this->client->request('PUT', $url, array_filter(array_merge(
                    $this->createRequestOptions(),
                    [
                        'body' => $file,
                        'headers' => array_merge(self::DEFAULT_HEADERS, $headers),
                        'progress' => $progressCallback,
                    ]
                )));
            });

            if ($progressBar instanceof ProgressBar) {
                $progressBar->finish();
            }
        } finally {
            if (is_resource($file)) {
                fclose($file);
            }
        }
    }

    /**
     * Create request options with timeouts.
     */
    private function createRequestOptions(): array
    {
        return [
            'connect_timeout' => $this->connectTimeout,
            'timeout' => $this->timeout,
        ];
    }

    /**
     * Retry callback a given number of times.
     */
    private function retry(callable $callback, $times = 5)
    {
        beginning:
        $times--;

        try {
            return $callback();
        } catch (\Throwable $exception) {
            if ($times < 1) {
                throw $exception;
            }

            sleep(1);

            goto beginning;
        }
    }

    /**
     * Retry a batch of requests with exponential backoff.
     */
    private function retryBatch(string $method, array $requests, ?ProgressBar $progressBar, int $attemptsRemaining, int $baseDelay = 1)
    {
        $failedRequests = [];
        $completedCount = 0;

        $requestGenerator = function () use ($method, $requests, &$failedRequests, &$completedCount, $progressBar) {
            foreach ($requests as $key => $request) {
                yield $key => function () use ($method, $request, $key, &$failedRequests, &$completedCount, $progressBar) {
                    return $this->client->requestAsync($method, $request['uri'], array_merge(
                        $this->createRequestOptions(),
                        [
                            'body' => $request['body'] ?? null,
                            'headers' => array_merge(self::DEFAULT_HEADERS, $request['headers']),
                        ]
                    ))->then(
                        function ($response) use (&$completedCount, $progressBar) {
                            ++$completedCount;
                            if ($progressBar instanceof ProgressBar) {
                                $progressBar->advance();
                            }

                            return $response;
                        },
                        function ($reason) use ($key, $request, &$failedRequests) {
                            $failedRequests[$key] = $request;

                            return $reason;
                        }
                    );
                };
            }
        };

        $pool = new Pool($this->client, $requestGenerator(), [
            'concurrency' => $this->concurrency,
        ]);
        $pool->promise()->wait();

        // Retry failed requests with exponential backoff
        if (!empty($failedRequests) && $attemptsRemaining > 0) {
            $delay = $baseDelay * (2 ** ($this->maxRetries - $attemptsRemaining));
            sleep(min($delay, 16)); // Cap at 16 seconds

            $this->retryBatch($method, $failedRequests, $progressBar, $attemptsRemaining - 1, $baseDelay);
        } elseif (!empty($failedRequests)) {
            throw new RuntimeException(sprintf('Failed to upload %d files after %d attempts', count($failedRequests), $this->maxRetries));
        }
    }
}
