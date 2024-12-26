select *
from layoffs;

CREATE TABLE layoffs_stag
LIKE layoffs;

INSERT layoffs_stag
select * 
from layoffs;

select *,
row_number() OVER 
(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date') AS row_num 
from layoffs_stag;

with duplicate_cte AS
(
select *,
row_number() OVER 
(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num 
from layoffs_stag
)

select * 
from layoffs_stag2
where row_num > 1

SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoffs_stag2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 1;

select company,trim(company)
from layoffs_stag2;

SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs_stag2
SET company = trim(company);

select *
from layoffs_stag2
where industry LIKE ('crypto%');

UPDATE layoffs_stag2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

select *
from layoffs_stag2
where industry LIKE ('crypto%');

select distinct country,trim(trailing '.' FROM country)
from layoffs_stag2
order by 2;

update layoffs_stag2
Set country = trim(trailing '.' FROM country)
where country like 'United States%';

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_stag2;

update layoffs_stag2
set `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_stag2
modify column `date` date;


select t1.industry,t2.industry
from layoffs_stag2 t1
join layoffs_stag2 t2
on t1.company=t2.company
where t1.industry is null 
and t2.industry IS  not null;

update layoffs_stag2 t1
join layoffs_stag2 t2
on t1.company=t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry IS not null;

select * 
from layoffs_stag2
where total_laid_off is null and
percentage_laid_off is null;

delete  
from layoffs_stag2
where  total_laid_off is null and
percentage_laid_off is null;

select * 
from layoffs_stag2;

alter table layoffs_stag2
drop column row_num;

select * 
from layoffs_stag2



 