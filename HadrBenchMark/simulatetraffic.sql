use AdventureWorks2017
go



--create procedure usp_generateIdentifier
--    @minLen int = 1
--    , @maxLen int = 128
--    , @seed int output
--    , @string varchar(8000) output
--as
--begin
--    set nocount on;
--    declare @length int;
--    declare @alpha varchar(8000)
--        , @digit varchar(8000)
--        , @specials varchar(8000)
--        , @first varchar(8000)
--    declare @step bigint = rand(@seed) * 2147483647;

--    select @alpha = 'qwertyuiopasdfghjklzxcvbnm'
--        , @digit = '1234567890'
--        , @specials = '_@# '
--    select @first = @alpha + '_@';

--    set  @seed = (rand((@seed+@step)%2147483647)*2147483647);

--    select @length = @minLen + rand(@seed) * (@maxLen-@minLen)
--        , @seed = (rand((@seed+@step)%2147483647)*2147483647);

--    declare @dice int;
--    select @dice = rand(@seed) * len(@first),
--        @seed = (rand((@seed+@step)%2147483647)*2147483647);
--    select @string = substring(@first, @dice, 1);

--    while 0 < @length 
--    begin
--        select @dice = rand(@seed) * 100
--            , @seed = (rand((@seed+@step)%2147483647)*2147483647);
--        if (@dice < 10) -- 10% special chars
--        begin
--            select @dice = rand(@seed) * len(@specials)+1
--                , @seed = (rand((@seed+@step)%2147483647)*2147483647);
--            select @string = @string + substring(@specials, @dice, 1);
--        end
--        else if (@dice < 10+10) -- 10% digits
--        begin
--            select @dice = rand(@seed) * len(@digit)+1
--                , @seed = (rand((@seed+@step)%2147483647)*2147483647);
--            select @string = @string + substring(@digit, @dice, 1);
--        end
--        else -- rest 80% alpha
--        begin
--            declare @preseed int = @seed;
--            select @dice = rand(@seed) * len(@alpha)+1
--                , @seed = (rand((@seed+@step)%2147483647)*2147483647);

--            select @string = @string + substring(@alpha, @dice, 1);
--        end

--        select @length = @length - 1;   
--    end
--end
--go
--create proc [dbo].uspRandChars
--    @len int,
--    @min tinyint = 48,
--    @range tinyint = 74,
--    @exclude varchar(50) = '0:;<=>?@O[]`^\/',
--    @output varchar(50) output
--as 
--    declare @char char
--    set @output = ''
 
--    while @len > 0 begin
--       select @char = char(round(rand() * @range + @min, 0))
--       if charindex(@char, @exclude) = 0 begin
--           set @output += @char
--           set @len = @len - 1
--       end
--    end
--;
--go

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


	update Production.Product
		set rowguid = NEWID()
		where ProductID = (select ROUND(RAND()*1000, 0))




	--DECLARE @cnt INT = 0;
	--while @cnt < 10
	--begin
		declare @seed int;
		declare @string varchar(128);

		select @seed = RAND()*1000; -- saved start seed


		exec usp_generateIdentifier 
			@seed = @seed output
			, @string = @string output;

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

end




