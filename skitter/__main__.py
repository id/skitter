import sys

if len(sys.argv) > 1 and sys.argv[1] == "chat":
    from skitter.cli import main

    main()
else:
    from skitter.coordinator import main

    main()
