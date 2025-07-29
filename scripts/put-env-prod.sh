scp ./.env* robmenning.com@xenodochial-turing.108-175-7-118.plesk.page:/var/www/vhosts/robmenning.com/bor/bor-workflow/

# To copy .env files from the production server back to your local machine (reverse of above):
scp robmenning.com@xenodochial-turing.108-175-7-118.plesk.page:/var/www/vhosts/robmenning.com/bor/bor-workflow/.env* ./bor-workflow/
