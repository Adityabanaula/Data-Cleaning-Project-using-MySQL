-- Data Cleaning

SELECT * 
FROM layoffs;

-- 1. Remove duplicates

-- 1.1. Creating a staging table cause I don't want to accidently delete original data
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- 1.2. Checking for duplicate data
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- 1.3. Checking if duplicate data is useful or not
SELECT * 
FROM layoffs_staging 
WHERE company = 'Casper';

-- 1.4. Creating another table to delete duplicates easily
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- 1.5. Deleting duplicates
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2;

-- 2. Standardizing data

-- 2.1. Removing blank space from company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- 2.2. Checking for any industry that might be same but spelt differently
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

-- Found them : multiple industries named as crypto or cryptocurrency which I think is the same
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- 2.3. Updating all the crypto or cryptocurrency industry to just crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 2.4. Checking the same for country
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

-- Found one : misspelt United States with a '.'
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- 2.5. Correcting the spelling
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 2.6. Date column has data as text
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- 2.7. Changing the data on date column from text to date
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- 2.8. Chaning the data type for date column to date instead of text
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Removing or populating null values or blank values

-- 3.1. Finding the null values or blank values

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Found multiple : Checking to see if data can be populated or not
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb';

-- 3.2. Checking for possible data that can be populated in place of null or blank
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL AND t2.industry != '');

-- 3.3. Populating the blank or null data
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL AND t2.industry != '');

-- 4. Remove any unnecessary columns

-- 4.1. Checking if the data in rows is useful or not
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4.2. Removing the unnecessary rows
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

-- 4.3. Removing the row_num column I added to check for duplicates
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- 5. Save the final data

CREATE TABLE layoffs_clean_data
LIKE layoffs_staging2;

INSERT INTO layoffs_clean_data
SELECT * 
FROM layoffs_staging2;

SELECT * 
FROM layoffs_clean_data;
