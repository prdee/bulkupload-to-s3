const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const { promisify } = require('util');
const chalk = require('chalk');
const ora = require('ora');
const cliProgress = require('cli-progress');

// Load environment variables
dotenv.config();

// Folders to upload (relative to current directory)
const FOLDERS_TO_UPLOAD = ['ExamGenrator', 'Grand', 'Keylinks'];

// Configuration
const config = {
  bucketName: process.env.AWS_S3_BUCKET_NAME || '',
  region: process.env.AWS_REGION || 'us-east-1',
  baseDir: process.cwd(), // Use current working directory as base
  concurrency: parseInt(process.env.CONCURRENCY || '20', 10),
  retryAttempts: parseInt(process.env.RETRY_ATTEMPTS || '3', 10),
  retryDelay: parseInt(process.env.RETRY_DELAY || '1000', 10)
};

// Check if AWS credentials and bucket name are provided
if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY || !config.bucketName) {
  console.error(chalk.red('Error: AWS credentials or bucket name not provided'));
  console.log(`
Please create a .env file with the following:
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region (defaults to us-east-1)
AWS_S3_BUCKET_NAME=your_bucket_name
CONCURRENCY=20 (optional, defaults to 20)
RETRY_ATTEMPTS=3 (optional, defaults to 3)
RETRY_DELAY=1000 (optional, defaults to 1000ms)
  `);
  process.exit(1);
}

// Initialize S3 client
const s3Client = new S3Client({
  region: config.region,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

// Progress tracking variables
let totalFiles = 0;
let uploadedFiles = 0;
let failedFiles = 0;
let skippedFiles = 0;
let startTime;

// Create a progress bar
const progressBar = new cliProgress.SingleBar({
  format: 'Uploading |' + chalk.cyan('{bar}') + '| {percentage}% | {value}/{total} Files | ETA: {eta}s | Speed: {speed} files/s',
  barCompleteChar: '\u2588',
  barIncompleteChar: '\u2591',
  hideCursor: true
});

// Function to get all files recursively in the specified folders
async function getAllFiles(folders) {
  const spinner = ora('Scanning folders for files...').start();
  const allFiles = [];
  
  async function scanDir(currentDir) {
    try {
      const entries = fs.readdirSync(currentDir, { withFileTypes: true });
      
      for (const entry of entries) {
        const fullPath = path.join(currentDir, entry.name);
        
        if (entry.isDirectory()) {
          await scanDir(fullPath);
        } else if (entry.isFile()) {
          allFiles.push(fullPath);
        }
      }
    } catch (error) {
      console.error(`Error scanning directory ${currentDir}: ${error.message}`);
    }
  }
  
  // Scan each folder
  for (const folder of folders) {
    const folderPath = path.join(config.baseDir, folder);
    if (fs.existsSync(folderPath)) {
      await scanDir(folderPath);
    } else {
      console.warn(chalk.yellow(`Warning: Folder '${folder}' does not exist and will be skipped.`));
    }
  }
  
  spinner.succeed(`Found ${chalk.green(allFiles.length)} files to upload from ${folders.length} folders`);
  return allFiles;
}

// Function to determine content type based on file extension
function getContentType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  
  const contentTypes = {
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png': 'image/png',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
    '.svg': 'image/svg+xml',
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.json': 'application/json',
    '.pdf': 'application/pdf',
    '.txt': 'text/plain',
    '.md': 'text/markdown',
    '.xml': 'application/xml',
    '.zip': 'application/zip',
    '.mp3': 'audio/mpeg',
    '.mp4': 'video/mp4',
    '.webm': 'video/webm',
    '.ico': 'image/x-icon',
    '.ttf': 'font/ttf',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2'
  };
  
  return contentTypes[ext] || 'application/octet-stream';
}

// Function to upload a single file
async function uploadFile(filePath, retryCount = 0) {
  try {
    // Read the file content instead of streaming
    const fileContent = fs.readFileSync(filePath);
    const relativeFilePath = path.relative(config.baseDir, filePath);
    const s3Key = relativeFilePath.replace(/\\/g, '/'); // Convert Windows backslashes to forward slashes
    
    const uploadParams = {
      Bucket: config.bucketName,
      Key: s3Key,
      Body: fileContent,
      ContentType: getContentType(filePath),
      ACL: 'public-read' // Make the object publicly accessible
    };

    await s3Client.send(new PutObjectCommand(uploadParams));
    uploadedFiles++;
    progressBar.increment({ speed: Math.round(uploadedFiles / ((Date.now() - startTime) / 1000)) });
    return true;
  } catch (error) {
    if (retryCount < config.retryAttempts) {
      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, config.retryDelay));
      return uploadFile(filePath, retryCount + 1);
    } else {
      failedFiles++;
      progressBar.increment({ speed: Math.round(uploadedFiles / ((Date.now() - startTime) / 1000)) });
      console.error(`\nFailed to upload ${chalk.red(filePath)}: ${error.message}`);
      return false;
    }
  }
}

// Function to process files in batches with controlled concurrency
async function processBatch(files) {
  // Process files in chunks to control concurrency
  const batchSize = config.concurrency;
  const batches = [];
  
  for (let i = 0; i < files.length; i += batchSize) {
    batches.push(files.slice(i, i + batchSize));
  }
  
  for (const batch of batches) {
    // Process each batch with controlled concurrency
    await Promise.all(batch.map(file => uploadFile(file)));
  }
}

// Main function
async function main() {
  console.log(chalk.bold.blue('S3 Bulk Folder Uploader'));
  console.log(chalk.gray('-----------------------------------'));
  console.log(`Bucket: ${chalk.yellow(config.bucketName)}`);
  console.log(`Region: ${chalk.yellow(config.region)}`);
  console.log(`Folders to Upload: ${chalk.yellow(FOLDERS_TO_UPLOAD.join(', '))}`);
  console.log(`Concurrency: ${chalk.yellow(config.concurrency)}`);
  console.log(chalk.gray('-----------------------------------'));
  
  try {
    // Check if at least one folder exists
    const existingFolders = FOLDERS_TO_UPLOAD.filter(folder => fs.existsSync(path.join(config.baseDir, folder)));
    
    if (existingFolders.length === 0) {
      console.error(chalk.red(`Error: None of the specified folders exist`));
      process.exit(1);
    }
    
    // Get all files from the specified folders
    const allFiles = await getAllFiles(FOLDERS_TO_UPLOAD);
    totalFiles = allFiles.length;
    
    if (totalFiles === 0) {
      console.log(chalk.yellow('No files found to upload.'));
      process.exit(0);
    }
    
    // Start the progress bar
    progressBar.start(totalFiles, 0, { speed: 0 });
    
    // Record start time
    startTime = Date.now();
    
    // Process files in batches with limited concurrency
    await processBatch(allFiles);
    
    // Stop the progress bar
    progressBar.stop();
    
    // Log results
    const duration = (Date.now() - startTime) / 1000;
    console.log(chalk.gray('-----------------------------------'));
    console.log(chalk.bold.green('\n✓ Upload Complete'));
    console.log(`Total Files: ${chalk.white(totalFiles)}`);
    console.log(`Uploaded: ${chalk.green(uploadedFiles)}`);
    console.log(`Failed: ${chalk.red(failedFiles)}`);
    console.log(`Duration: ${chalk.white(Math.round(duration))} seconds`);
    console.log(`Average Speed: ${chalk.white(Math.round(uploadedFiles / duration))} files/second`);
    
    // Generate public URL prefix
    const bucketUrl = `https://${config.bucketName}.s3.${config.region}.amazonaws.com/`;
    console.log(chalk.gray('-----------------------------------'));
    console.log(chalk.bold.blue('\nAccess Information:'));
    console.log(`Public URL prefix: ${chalk.green(bucketUrl)}`);
    
    // Show example URLs for each folder
    console.log(chalk.bold('\nExample URLs for uploaded folders:'));
    for (const folder of existingFolders) {
      console.log(`${folder}: ${chalk.green(bucketUrl + folder + '/')}`);
    }
    
    if (allFiles.length > 0) {
      const firstFile = path.relative(config.baseDir, allFiles[0]).replace(/\\/g, '/');
      console.log(`\nExample file URL: ${chalk.green(bucketUrl + firstFile)}`);
    }
  } catch (error) {
    console.error(chalk.red(`Error: ${error.message}`));
    process.exit(1);
  }
}

// Run the script
main();