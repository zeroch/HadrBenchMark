
--CREATE PROCEDURE USP_GENERATEIDENTIFIER
--    @MINLEN INT = 1
--    , @MAXLEN INT = 128
--    , @SEED INT OUTPUT
--    , @STRING VARCHAR(8000) OUTPUT
--AS
--BEGIN
--    SET NOCOUNT ON;
--    DECLARE @LENGTH INT;
--    DECLARE @ALPHA VARCHAR(8000)
--        , @DIGIT VARCHAR(8000)
--        , @SPECIALS VARCHAR(8000)
--        , @FIRST VARCHAR(8000)
--    DECLARE @STEP BIGINT = RAND(@SEED) * 2147483647;

--    SELECT @ALPHA = 'QWERTYUIOPASDFGHJKLZXCVBNM'
--        , @DIGIT = '1234567890'
--        , @SPECIALS = '_@# '
--    SELECT @FIRST = @ALPHA + '_@';

--    SET  @SEED = (RAND((@SEED+@STEP)%2147483647)*2147483647);

--    SELECT @LENGTH = @MINLEN + RAND(@SEED) * (@MAXLEN-@MINLEN)
--        , @SEED = (RAND((@SEED+@STEP)%2147483647)*2147483647);

--    DECLARE @DICE INT;
--    SELECT @DICE = RAND(@SEED) * LEN(@FIRST),
--        @SEED = (RAND((@SEED+@STEP)%2147483647)*2147483647);
--    SELECT @STRING = SUBSTRING(@FIRST, @DICE, 1);

--    WHILE 0 < @LENGTH 
--    BEGIN
--        SELECT @DICE = RAND(@SEED) * 100
--            , @SEED = (RAND((@SEED+@STEP)%2147483647)*2147483647);
--        IF (@DICE < 10) -- 10% SPECIAL CHARS
--        BEGIN
--            SELECT @DICE = RAND(@SEED) * LEN(@SPECIALS)+1
--                , @SEED = (RAND((@SEED+@STEP)%2147483647)*2147483647);
--            SELECT @STRING = @STRING + SUBSTRING(@SPECIALS, @DICE, 1);
--        END
--        ELSE IF (@DICE < 10+10) -- 10% DIGITS
--        BEGIN
--            SELECT @DICE = RAND(@SEED) * LEN(@DIGIT)+1
--                , @SEED = (RAND((@SEED+@STEP)%2147483647)*2147483647);
--            SELECT @STRING = @STRING + SUBSTRING(@DIGIT, @DICE, 1);
--        END
--        ELSE -- REST 80% ALPHA
--        BEGIN
--            DECLARE @PRESEED INT = @SEED;
--            SELECT @DICE = RAND(@SEED) * LEN(@ALPHA)+1
--                , @SEED = (RAND((@SEED+@STEP)%2147483647)*2147483647);

--            SELECT @STRING = @STRING + SUBSTRING(@ALPHA, @DICE, 1);
--        END

--        SELECT @LENGTH = @LENGTH - 1;   
--    END
--END
--GO
--CREATE PROC [DBO].USPRANDCHARS
--    @LEN INT,
--    @MIN TINYINT = 48,
--    @RANGE TINYINT = 74,
--    @EXCLUDE VARCHAR(50) = '0:;<=>?@O[]`^\/',
--    @OUTPUT VARCHAR(50) OUTPUT
--AS 
--    DECLARE @CHAR CHAR
--    SET @OUTPUT = ''
 
--    WHILE @LEN > 0 BEGIN
--       SELECT @CHAR = CHAR(ROUND(RAND() * @RANGE + @MIN, 0))
--       IF CHARINDEX(@CHAR, @EXCLUDE) = 0 BEGIN
--           SET @OUTPUT += @CHAR
--           SET @LEN = @LEN - 1
--       END
--    END
--;
--GO

--declare @newpwd varchar(20)


---- all values between ASCII code 48 - 122 excluding defaults
--exec [dbo].uspRandChars @len=8, @output=@newpwd out
--select @newpwd


---- all lower case letters excluding o and l
--exec [dbo].uspRandChars @len=10, @min=97, @range=25, @exclude='ol', @output=@newpwd out
--select @newpwd


---- all upper case letters excluding O
--exec [dbo].uspRandChars @len=12, @min=65, @range=25, @exclude='O', @output=@newpwd out
--select @newpwd


---- all numbers between 0 and 9
--exec [dbo].uspRandChars @len=14, @min=48, @range=9, @exclude='', @output=@newpwd out
--select @newpwd

select * from Production.Product

declare @counter int
set @counter = 1

while 1=1
begin


	UPDATE Production.Product  
		SET ListPrice = (select ROUND(RAND()*500, 0))

	-- remove this this change and wait for 0.001 is about the 900-1000 transaction / sec
	--update Production.Product
	--	set rowguid = NEWID()
	--	where ProductID = (select ROUND(RAND()*1000, 0))




	--DECLARE @cnt INT = 0;
	--while @cnt < 10
	--begin
		declare @seed int;
		declare @string varchar(128);

		select @seed = RAND()*1000; -- saved start seed
		--print @seed

		exec usp_generateIdentifier 
			@seed = @seed output
			, @string = @string output;
		--print @string;  
		update Person.Password
		set PasswordHash = @string
			where BusinessEntityID = (select ROUND(RAND()*20777, 0));
	--	set @cnt = @cnt +1;
	--end



	--SET @cnt = 0;
	--while @cnt < 10
	--begin
		declare @newcard varchar(25)
		-- all numbers between 0 and 9
		exec [dbo].uspRandChars @len=16, @min=48, @range=9, @exclude='', @output=@newcard out

		update Sales.CreditCard
			set CardNumber = @newcard
			where CreditCardID = (select ROUND(RAND()*18000, 0))
	--	set @cnt = @cnt +1;
	--end

	set @counter = @counter +1
	--print @counter

	-- no wait is about 1500 transaction / sec
	waitfor delay '00:00:0.001'
end




