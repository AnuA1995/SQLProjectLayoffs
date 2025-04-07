-- Data Cleaning usable format, fix raw data to use for data visulaizations

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any columns 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


SELECT *,
row_number() OVER (
PARTITION BY company,industry,total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

with duplicate_cte as 
(
SELECT *,
row_number() OVER (
PARTITION BY company,location,industry,total_laid_off, percentage_laid_off, `date`,stage,country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
where row_num > 1;

SELECT *
FROM layoffs_staging
where company = 'Casper' ;





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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
row_number() OVER (
PARTITION BY company,location,industry,total_laid_off, percentage_laid_off, `date`,stage,country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

DELETE
FROM layoffs_staging2
where row_num > 1;

SELECT *
FROM layoffs_staging2;


-- Standardizing data

SELECT distinct(TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'CRYP%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'CRYP%';

select industry
from layoffs_staging2
where industry = 'Crypto';


SELECT distinct country ,country
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
where country LIKE 'United States%';

select `date` , 
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` =  str_to_date(`date`, '%m/%d/%Y');

select `date` 
from layoffs_staging2;

alter table layoffs_staging2
modify column date DATE;

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null; 

select *
from layoffs_staging2
where industry is null
or industry = '' ;


select *
from layoffs_staging2
where company like 'Ball%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t2
join layoffs_staging2 t1
		on t1.company = t2.company
where (t1.industry IS NULL or t1.industry = '')
AND t2.industry is not null;



UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';


UPDATE layoffs_staging2 t1
join layoffs_staging2 t2
		on t1.company = t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry is not null;


SELECT * 
from layoffs_staging2;



DELETE 
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null; 


SELECT * 
FROM layoffs_staging2; 

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- Exploratory Data Analysis 

SELECT *
FROM layoffs_staging2
where percentage_laid_off = 1;


SELECT max(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2;



SELECT *
FROM layoffs_staging2
where percentage_laid_off = 1
ORDER BY funds_raised_millions  DESC;

SELECT company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


SELECT MIN(`date`), Max(`date`)
from layoffs_staging2;


SELECT country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;


SELECT YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by YEAR(`date`)
order by 1 desc;



